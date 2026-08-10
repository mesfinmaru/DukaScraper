"""
LLM Intelligence Worker

Reads parsed content from Kafka (crawl.parsed topic), performs LLM analysis
using local Qwen model via Ollama, and writes intelligence analytics to ClickHouse.

Pipeline:
  crawl.parsed (Kafka) → [LLM Worker] → intelligence_analytics (ClickHouse)
                          ↓
                      PostgreSQL: update parsed_items.intelligence_processed = true
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Optional

import clickhouse_connect
import re
import httpx
from aiokafka import AIOKafkaConsumer

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.common.constants.intelligence_categories import (
    ALL_INTELLIGENCE_CATEGORIES,
    CATEGORY_DESCRIPTIONS,
    DEFAULT_INTELLIGENCE_CATEGORY,
)
from app.pipeline.schemas import ParsedItem, IntelligenceAnalytics
from app.storage.postgres.client import pg_client

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
CONSUME_TOPIC = settings.crawl_parsed_topic  # crawl.parsed
WORKER_TYPE = "llm-worker"

# --- Ollama Configuration ---
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2:8b")
MAX_CONCURRENT_TASKS = 4
llm_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# --- ClickHouse Configuration ---
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_HTTP_PORT = int(os.getenv("CH_PORT", str(settings.CLICKHOUSE_HTTP_PORT)))
CH_USER = os.getenv("CH_USER", settings.CLICKHOUSE_USER)
CH_PASSWORD = os.getenv("CH_PASSWORD", settings.CLICKHOUSE_PASSWORD or "")
CH_DATABASE = os.getenv("CH_DATABASE", "duka_scraper")

# --- Initialize ClickHouse Client (HTTP interface via clickhouse_connect, same as exporter-worker) ---
try:
    ch_client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_HTTP_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=False,
        verify=False,
    )
    logger.info(f"ClickHouse client initialized: {CH_HOST}:{CH_HTTP_PORT}/{CH_DATABASE}")
except Exception as e:
    logger.error(f"Failed to initialize ClickHouse client: {e}")
    ch_client = None


# ============================================================================
# OLLAMA LLM INTERFACE
# ============================================================================


class OllamaLLMClient:
    """Interface to Ollama for local LLM inference."""

    def __init__(self, base_url: str, model: str):
        self.base_url = base_url
        self.model = model
        self.generate_url = f"{base_url}/api/generate"
        self.client = httpx.AsyncClient(timeout=60)

    async def close(self) -> None:
        await self.client.aclose()

    async def is_available(self) -> bool:
        """Check if Ollama service is running."""
        try:
            response = await self.client.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Ollama not available: {e}")
            return False

    async def extract_intelligence(self, parsed_text: str, url: str) -> dict:
        """
        Use LLM to extract actionable intelligence from parsed content.

        Args:
            parsed_text: Clean extracted text from parsing phase
            url: Source URL

        Returns:
            Dictionary with keys: category, threat_severity, entities, summary, llm_score
        """
        if not parsed_text or len(parsed_text.strip()) < 50:
            logger.warning(f"Parsed text too short for analysis: {len(parsed_text)} chars")
            return self._empty_intelligence()

        # Construct prompt for LLM
        category_list_str = "\n".join(
            f'  - "{cat}": {desc}' for cat, desc in CATEGORY_DESCRIPTIONS.items()
        )
        prompt = f"""Analyze this scraped content for actionable intelligence.

Source URL: {url}

Content:
{parsed_text[:2000]}

Classify into exactly ONE of these categories:
{category_list_str}

Please respond ONLY with a valid JSON object (no markdown, no explanation) containing:
{{
  "category": one of {sorted(ALL_INTELLIGENCE_CATEGORIES)},
  "threat_severity": integer from 1-5 (1=low, 5=critical),
  "entities": list of extracted entities (names, emails, IPs, domains, sensitive info),
  "summary": brief summary of findings (max 200 chars)
}}"""

        try:
            response = await self.client.post(
                self.generate_url,
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "temperature": 0.3,  # Low temperature for consistency
                },
                timeout=60,
            )

            if response.status_code != 200:
                # If model is not found, attempt to fallback to a lightweight rule-based analyzer.
                logger.error(f"Ollama error: {response.status_code} - {response.text}")
                try:
                    body_text = response.text or ""
                except Exception:
                    body_text = ""

                if response.status_code == 404 or "model" in body_text.lower() and "not found" in body_text.lower():
                    logger.warning("Ollama model not found; using fallback analyzer.")
                    return self._fallback_analyze(parsed_text, url)

                return self._empty_intelligence()

            result = response.json()
            raw_response = result.get("response", "")
            if isinstance(raw_response, list):
                response_text = "\n".join(str(item) for item in raw_response)
            else:
                response_text = str(raw_response).strip()

            if not response_text:
                response_text = str(result.get("output") or result.get("text") or "").strip()

            if not response_text:
                logger.error("Empty response text from Ollama")
                return self._empty_intelligence()

            intelligence = self._parse_intelligence_response(response_text)
            intelligence = self._validate_intelligence(intelligence)
            intelligence["llm_score"] = self._extract_llm_score(result, response_text)

            logger.info(f"LLM analysis complete: {intelligence['category']} (severity: {intelligence['threat_severity']})")
            return intelligence

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {e}")
            return self._empty_intelligence()
        except httpx.HTTPError as e:
            logger.error(f"Ollama request failed: {e}")
            return self._empty_intelligence()

    @staticmethod
    def _parse_intelligence_response(response_text: str) -> dict:
        """Parse the Ollama response text into structured intelligence."""
        cleaned_text = response_text.strip()
        if cleaned_text.startswith("{") and cleaned_text.endswith("}"):
            try:
                return json.loads(cleaned_text)
            except json.JSONDecodeError:
                pass

        match = re.search(r"(\{.*\})", cleaned_text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        extracted = {}
        for line in cleaned_text.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            extracted[key.strip().strip('"').strip("'")] = value.strip().strip('"').strip("'")
        return extracted

    def _fallback_analyze(self, parsed_text: str, url: str) -> dict:
        """Simple rule-based fallback analyzer used when Ollama model is unavailable.

        This provides reasonable default category/summary/entities so the pipeline
        continues producing analytics even without a working LLM.
        """
        text = (parsed_text or "").strip()
        summary = (text[:180] + "...") if len(text) > 180 else text

        # Extract simple entities: emails, domains, IPs
        entities = []
        try:
            entities += re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
            entities += re.findall(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", text)
            domains = re.findall(r"(?:https?://)?([A-Za-z0-9.-]+\.[A-Za-z]{2,})", text)
            entities += domains
        except Exception:
            pass

        # Heuristic category selection by keywords
        lowered = text.lower()
        category = DEFAULT_INTELLIGENCE_CATEGORY
        if any(k in lowered for k in ("attack", "bomb", "explosion", "violent", "kill")):
            category = "threat"
        elif any(k in lowered for k in ("policy", "government", "minister", "parliament")):
            category = "political"
        elif any(k in lowered for k in ("health", "hospital", "disease", "covid", "vaccine")):
            category = "health"

        return {
            "category": category,
            "threat_severity": 1,
            "entities": list(dict.fromkeys([e for e in entities if e])),
            "summary": summary[:200] if summary else "No summary available",
            "llm_score": 0.1,
        }

    @staticmethod
    def _extract_llm_score(result: dict, response_text: str) -> float:
        try:
            if "eval_count" in result and "eval_duration" in result:
                return float(result.get("eval_count", 0)) / max(float(result.get("eval_duration", 1)), 1)
            if "score" in result:
                return float(result.get("score", 0.0))
            if "llm_score" in result:
                return float(result.get("llm_score", 0.0))
        except Exception:
            pass
        return 1.0 if response_text else 0.0

    @staticmethod
    def _validate_intelligence(data: dict) -> dict:
        """Validate and normalize LLM output."""
        category = data.get("category", DEFAULT_INTELLIGENCE_CATEGORY)
        if category not in ALL_INTELLIGENCE_CATEGORIES:
            category = DEFAULT_INTELLIGENCE_CATEGORY

        raw_entities = data.get("entities", [])
        if isinstance(raw_entities, str):
            entities = [e.strip() for e in re.split(r"[\n,;]+", raw_entities) if e.strip()]
        elif isinstance(raw_entities, list):
            entities = [str(e).strip() for e in raw_entities if str(e).strip()]
        else:
            entities = []

        summary = str(data.get("summary") or data.get("description") or "").strip()
        if not summary:
            summary = "No summary provided by LLM."

        return {
            "category": category,
            "threat_severity": max(1, min(5, int(data.get("threat_severity", 1)))),  # Clamp 1-5
            "entities": entities[:50],  # Max 50 entities
            "summary": summary[:200],  # Max 200 chars
        }

    @staticmethod
    def _empty_intelligence() -> dict:
        """Return empty/safe intelligence object."""
        return {
            "category": DEFAULT_INTELLIGENCE_CATEGORY,
            "threat_severity": 0,
            "entities": [],
            "summary": "Unable to analyze",
            "llm_score": 0.0,
        }


# Initialize Ollama client
ollama_client = OllamaLLMClient(OLLAMA_BASE_URL, OLLAMA_MODEL)


# ============================================================================
# CLICKHOUSE INGEST
# ============================================================================


async def ingest_intelligence_to_clickhouse(intelligence: IntelligenceAnalytics):
    """
    Insert intelligence record into ClickHouse intelligence_analytics table.

    Args:
        intelligence: IntelligenceAnalytics object
    """
    if not ch_client:
        logger.error("ClickHouse client not available, skipping ingest")
        return

    try:
        # Use a fresh ClickHouse client per thread to avoid concurrent session errors.
        def _do_insert():
            client = clickhouse_connect.get_client(
                host=CH_HOST,
                port=CH_HTTP_PORT,
                username=CH_USER,
                password=CH_PASSWORD or "",
                database=CH_DATABASE,
                secure=False,
                verify=False,
            )
            try:
                client.insert(
                    "intelligence_analytics",
                    [[
                        intelligence.job_id,
                        intelligence.item_id,
                        intelligence.url,
                        intelligence.source_type,
                        intelligence.category,
                        intelligence.threat_severity,
                        intelligence.entities,
                        intelligence.summary,
                        intelligence.language,
                        intelligence.llm_model,
                        intelligence.llm_score,
                        intelligence.created_at,
                    ]],
                    column_names=[
                        "job_id", "item_id", "url", "source_type", "category", "threat_severity",
                        "entities", "summary", "language", "llm_model", "llm_score", "created_at",
                    ],
                )
            finally:
                try:
                    client.close()
                except Exception:
                    pass

        await asyncio.to_thread(_do_insert)
        logger.info(f"Ingested intelligence record: job={intelligence.job_id} item={intelligence.item_id}")

    except Exception as e:
        logger.error(f"Failed to ingest to ClickHouse: {e}")


# ============================================================================
# MESSAGE PROCESSING
# ============================================================================


async def process_parsed_item(message_value: bytes):
    """
    Process a parsed item from Kafka, perform LLM analysis, and store results.

    Args:
        message_value: Raw Kafka message value (JSON)
    """
    try:
        data = json.loads(message_value)
        parsed_item = ParsedItem(**data)

        logger.info(f"Processing parsed item: job={parsed_item.job_id} item={parsed_item.item_id} url={parsed_item.url}")

        # Extract text from parsed data
        if isinstance(parsed_item.data, dict):
            extracted_text = parsed_item.data.get("extracted_text", "")
        else:
            extracted_text = parsed_item.data.extracted_text

        # Perform LLM analysis
        intelligence_data = await ollama_client.extract_intelligence(extracted_text, parsed_item.url)

        # Create intelligence analytics record
        intelligence = IntelligenceAnalytics(
            job_id=parsed_item.job_id,
            item_id=parsed_item.item_id,
            url=parsed_item.url,
            source_type=intelligence_data.get("category", DEFAULT_INTELLIGENCE_CATEGORY),  # Inferred source_type
            category=intelligence_data.get("category", DEFAULT_INTELLIGENCE_CATEGORY),
            threat_severity=intelligence_data.get("threat_severity", 0),
            entities=intelligence_data.get("entities", []),
            summary=intelligence_data.get("summary", ""),
            language=parsed_item.language,
            llm_model=OLLAMA_MODEL,
            llm_score=intelligence_data.get("llm_score", 0.0),
        )

        # Ingest to ClickHouse
        await ingest_intelligence_to_clickhouse(intelligence)

        # Update PostgreSQL: mark as intelligence_processed
        try:
            await pg_client.mark_item_intelligence_processed(parsed_item.item_id)
            logger.debug(f"Updated PostgreSQL: item {parsed_item.item_id} marked as intelligence_processed")
        except Exception as e:
            logger.warning(f"Failed to update PostgreSQL for item {parsed_item.item_id}: {e}")

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing parsed item: {e}", exc_info=True)


async def process_message_safely(message_value: bytes):
    async with llm_semaphore:
        await process_parsed_item(message_value)


async def main():
    """Main LLM worker lifecycle loop."""
    # Check Ollama availability
    if not await ollama_client.is_available():
        logger.error(f"Ollama service not available at {OLLAMA_BASE_URL}. Exiting.")
        sys.exit(1)

    logger.info(f"Ollama service available. Using model: {OLLAMA_MODEL}")

    await pg_client.connect()

    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info(f"'{WORKER_TYPE}' worker online listening on topic '{CONSUME_TOPIC}'.")

    loop = asyncio.get_running_loop()
    current_task = asyncio.current_task()

    def _stop() -> None:
        if current_task:
            current_task.cancel()

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                pass

    try:
        async for msg in consumer:
            asyncio.create_task(process_message_safely(msg.value))
    except asyncio.CancelledError:
        logger.info("LLM worker cancellation requested.")
    finally:
        logger.info("Shutting down LLM worker gracefully...")
        await consumer.stop()
        await pg_client.close()
        await ollama_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("LLM worker execution interrupted by user.")
