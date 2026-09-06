"""
LLM Intelligence Worker

Reads parsed content from Kafka (crawl.parsed topic), performs LLM analysis
using a hosted or local LLM provider, and writes intelligence analytics to ClickHouse.

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

try:
    from groq import Groq
except Exception:  # pragma: no cover - optional dependency fallback
    Groq = None

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
from app.common.constants.source_types import ALL_SOURCE_TYPES, DEFAULT_SOURCE_TYPE
from app.pipeline.schemas import ParsedItem, IntelligenceAnalytics
from app.storage.postgres.client import pg_client
from workers.common import build_unique_crawl_object_name as _shared_build_unique_crawl_object_name


def build_unique_crawl_object_name(job_id: str, url: str, *, extension: str = ".json") -> str:
    """Collision-safe raw crawl naming for a single page within a job."""
    return _shared_build_unique_crawl_object_name(job_id, url, extension=extension)

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

# --- LLM Configuration ---
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "hosted").strip().lower()
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2:8b")
HOSTED_LLM_URL = os.getenv("HOSTED_LLM_URL", "")
HOSTED_LLM_API_KEY = os.getenv("HOSTED_LLM_API_KEY", "")
HOSTED_LLM_MODEL = os.getenv("HOSTED_LLM_MODEL", "google-gemini-flash")
FALLBACK_ONLY = os.getenv("FALLBACK_ONLY", "false").strip().lower() in ("1", "true", "yes")
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

    @staticmethod
    def _fallback_analyze(parsed_text: str, url: str) -> dict:
        """Simple rule-based fallback analyzer used when Ollama model is unavailable.

        This provides reasonable default category/summary/entities so the pipeline
        continues producing analytics even without a working LLM.
        """
        text = (parsed_text or "").strip()
        summary = (text[:600] + "...") if len(text) > 600 else text

        # Extract simple entities: emails, domains, IPs
        entities = []
        try:
            entities += re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
            entities += re.findall(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", text)
            domains = re.findall(r"(?:https?://)?([A-Za-z0-9.-]+\.[A-Za-z]{2,})", text)
            entities += domains
        except Exception:
            pass

        lowered = text.lower()
        category = DEFAULT_INTELLIGENCE_CATEGORY
        if any(k in lowered for k in ("attack", "bomb", "explosion", "violent", "kill", "ransomware", "malware", "exploit")):
            category = "cyber_threat"
        elif any(k in lowered for k in ("policy", "government", "minister", "parliament", "election", "court", "agency")):
            category = "gov_issue"
        elif any(k in lowered for k in ("health", "hospital", "disease", "covid", "vaccine")):
            category = "other"

        if any(k in url.lower() for k in ("news", "article", "press", "breaking")):
            source_type = "news"
        elif any(k in url.lower() for k in ("forum", "discussion", "board")):
            source_type = "forum"
        elif any(k in url.lower() for k in ("blog", "post")):
            source_type = "blog"
        elif any(k in url.lower() for k in ("shop", "product", "store")):
            source_type = "ecommerce"
        else:
            source_type = DEFAULT_SOURCE_TYPE

        return {
            "source_type": source_type,
            "category": category,
            "threat_severity": 1,
            "entities": list(dict.fromkeys([e for e in entities if e])),
            "summary": summary if summary else "No summary available",
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

        source_type = str(data.get("source_type") or DEFAULT_SOURCE_TYPE).strip().lower()
        if source_type not in ALL_SOURCE_TYPES:
            source_type = DEFAULT_SOURCE_TYPE

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

        raw_severity = data.get("threat_severity", 1)
        try:
            threat_severity = int(raw_severity)
        except (TypeError, ValueError):
            threat_severity = 1

        if threat_severity < 1:
            threat_severity = 1
        elif threat_severity > 5:
            threat_severity = 5

        return {
            "source_type": source_type,
            "category": category,
            "threat_severity": threat_severity,
            "entities": entities[:100],
            "summary": summary,
        }

    @staticmethod
    def _empty_intelligence() -> dict:
        """Return empty/safe intelligence object."""
        return {
            "source_type": DEFAULT_SOURCE_TYPE,
            "category": DEFAULT_INTELLIGENCE_CATEGORY,
            "threat_severity": 1,
            "entities": [],
            "summary": "Unable to analyze",
            "llm_score": 0.0,
        }


class HostedLLMClient:
    """Generic hosted LLM client for Google AI and other hosted endpoints.

    Handles Google Generative API auth and response scraping, while reusing
    local response parsing helpers from OllamaLLMClient.
    """

    def __init__(self, base_url: str, api_key: str = "", model: str = ""):
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.api_key = api_key
        self.model = model
        self.provider = self._detect_provider(self.base_url)
        self.client = httpx.AsyncClient(timeout=120)
        self.groq_client = None
        if self.provider == "groq" and api_key and Groq is not None:
            self.groq_client = Groq(api_key=api_key)

    @staticmethod
    def _detect_provider(base_url: str) -> str:
        if not base_url:
            return "generic"
        lower = base_url.lower()
        if "groq.com" in lower:
            return "groq"
        if "generativelanguage.googleapis.com" in lower or "aiplatform.googleapis.com" in lower:
            return "google"
        return "generic"

    @staticmethod
    def _extract_text_from_payload(data):
        if data is None:
            return ""
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            if "content" in data and isinstance(data["content"], str):
                return data["content"]
            if "text" in data and isinstance(data["text"], str):
                return data["text"]
            if "message" in data:
                return HostedLLMClient._extract_text_from_payload(data["message"])
            if "choices" in data:
                for choice in data["choices"]:
                    result = HostedLLMClient._extract_text_from_payload(choice)
                    if result:
                        return result
            for value in data.values():
                result = HostedLLMClient._extract_text_from_payload(value)
                if result:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = HostedLLMClient._extract_text_from_payload(item)
                if result:
                    return result
        return ""

    async def close(self) -> None:
        await self.client.aclose()

    @staticmethod
    def _detect_content_language(text: str) -> str:
        if not text:
            return "en"
        text = text.strip()
        if len(text) < 20:
            return "en"
        amharic_chars = sum(1 for ch in text if "\u1200" <= ch <= "\u137f" or "\u1380" <= ch <= "\u139f")
        english_chars = sum(1 for ch in text if ch.isascii() and ch.isalpha())
        total_alpha = sum(1 for ch in text if ch.isalpha())
        if total_alpha == 0:
            return "en"
        amharic_ratio = amharic_chars / total_alpha if total_alpha else 0.0
        english_ratio = english_chars / total_alpha if total_alpha else 0.0
        if amharic_ratio >= 0.4 and amharic_ratio > english_ratio:
            return "am"
        return "en"

    async def is_available(self) -> bool:
        if not self.base_url:
            return False
        if "generativelanguage.googleapis.com" in self.base_url or "aiplatform.googleapis.com" in self.base_url:
            # Google Generative API endpoints may reject HEAD, so trust the URL and API key if configured.
            return bool(self.api_key)
        if "groq.com" in self.base_url.lower():
            # Groq chat completions endpoint does not support HEAD and may 404 on HEAD checks even when valid.
            return bool(self.api_key)

        try:
            response = await self.client.head(self.base_url, timeout=5)
            return response.status_code in (200, 204, 301, 302)
        except Exception:
            return False

    async def extract_intelligence(self, parsed_text: str, url: str) -> dict:
        if not parsed_text or len(parsed_text.strip()) < 50:
            return {
                "source_type": DEFAULT_SOURCE_TYPE,
                "category": DEFAULT_INTELLIGENCE_CATEGORY,
                "threat_severity": 1,
                "entities": [],
                "summary": "Unable to analyze reliably; fallback inference used.",
                "llm_score": 0.0,
            }

        if not self.api_key and FALLBACK_ONLY:
            logger.warning("No hosted API key available; using fallback analyzer in fallback-only mode.")
            return OllamaLLMClient._fallback_analyze(parsed_text=parsed_text, url=url)

        category_list_str = "\n".join(
            f'  - "{cat}": {desc}' for cat, desc in CATEGORY_DESCRIPTIONS.items()
        )
        source_type_values = "news, forum, blog, social, government, academic, ecommerce, other"
        content_language = HostedLLMClient._detect_content_language(parsed_text)
        summary_language = "Amharic" if content_language == "am" else "English"
        max_analysis_chars = 2500
        if len(parsed_text) <= max_analysis_chars:
            full_content = parsed_text
        else:
            half = max_analysis_chars // 2
            full_content = f"{parsed_text[:half]}\n\n[...middle omitted for provider limit...]\n\n{parsed_text[-half:]}"
        prompt = f"""You are a careful content analyst. Read the full content below, not just the headline or first paragraph.

Source URL: {url}

Goal:
- Summarize the main idea, key facts, and why the content matters.
- This is not limited to intelligence-related content; summarize any meaningful article, post, forum discussion, announcement, report, or public update.
- If the content is primarily Amharic, write the summary in Amharic. Otherwise write the summary in English.
- Do not invent facts. Use only what is present in the content.
- Ignore boilerplate, ads, tracking links, and repeated site navigation noise.

Content language: {summary_language}

Content:
{full_content}

Classify into exactly ONE intelligence category:
{category_list_str}

Also classify the source type as one of: {source_type_values}

Return ONLY valid JSON with this exact structure:
{{
  "source_type": one of [{source_type_values}],
  "category": one of {sorted(ALL_INTELLIGENCE_CATEGORIES)},
  "threat_severity": integer from 1-5 (1=low, 5=critical),
  "entities": list of extracted entities (names, emails, IPs, domains, sensitive info, locations),
  "summary": a concise 2-4 sentence summary in {summary_language} of the main idea, most important facts, and why it matters
}}"""

        headers = {}
        body = None

        if self.provider == "google":
            if self.api_key:
                headers["X-goog-api-key"] = self.api_key
            body = {"contents": [{"parts": [{"text": prompt}]}]}
        elif self.provider == "groq":
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
                headers["Content-Type"] = "application/json"
            body = {
                "model": self.model or "openai/gpt-oss-120b",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
                "max_completion_tokens": 1024,
                "top_p": 1,
                "stream": False,
                "stop": None,
            }
        else:
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            body = {"model": self.model or OLLAMA_MODEL, "prompt": prompt}

        try:
            if self.provider == "groq" and self.groq_client is not None:
                completion = self.groq_client.chat.completions.create(
                    model=self.model or "openai/gpt-oss-120b",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_completion_tokens=1024,
                    top_p=1,
                    reasoning_effort="medium",
                    stream=False,
                    stop=None,
                )
                data = completion.model_dump() if hasattr(completion, "model_dump") else completion
                response_text = self._extract_text_from_payload(data)
            else:
                resp = await self.client.post(self.base_url, json=body, headers=headers, timeout=120)
                text = resp.text or ""
                try:
                    data = resp.json()
                except Exception:
                    data = {"response": text}

                response_text = self._extract_text_from_payload(data)
                if not response_text:
                    response_text = text

            intelligence = None
            try:
                intelligence = OllamaLLMClient._parse_intelligence_response(response_text)
                intelligence = OllamaLLMClient._validate_intelligence(intelligence)
                intelligence["llm_score"] = OllamaLLMClient._extract_llm_score({"response": response_text}, response_text)
            except Exception:
                intelligence = {
                    "source_type": DEFAULT_SOURCE_TYPE,
                    "category": DEFAULT_INTELLIGENCE_CATEGORY,
                    "threat_severity": 1,
                    "entities": [],
                    "summary": response_text if response_text else "Unable to analyze reliably; fallback inference used.",
                    "llm_score": 0.0,
                }

            return intelligence
        except Exception as e:
            logger.warning(f"Hosted LLM request failed: {e}")
            return OllamaLLMClient._fallback_analyze(parsed_text=parsed_text, url=url)


# Initialize LLM client
if LLM_PROVIDER == "ollama":
    llm_client = OllamaLLMClient(OLLAMA_BASE_URL, OLLAMA_MODEL)
else:
    llm_client = HostedLLMClient(HOSTED_LLM_URL, HOSTED_LLM_API_KEY, HOSTED_LLM_MODEL)


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
        intelligence_data = await llm_client.extract_intelligence(extracted_text, parsed_item.url)
        llm_model = HOSTED_LLM_MODEL if LLM_PROVIDER != "ollama" else OLLAMA_MODEL

        # Create intelligence analytics record
        intelligence = IntelligenceAnalytics(
            job_id=parsed_item.job_id,
            item_id=parsed_item.item_id,
            url=parsed_item.url,
            source_type=intelligence_data.get("source_type", DEFAULT_SOURCE_TYPE),
            category=intelligence_data.get("category", DEFAULT_INTELLIGENCE_CATEGORY),
            threat_severity=intelligence_data.get("threat_severity", 0),
            entities=intelligence_data.get("entities", []),
            summary=intelligence_data.get("summary", ""),
            language=parsed_item.language,
            llm_model=llm_model,
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
    if not await llm_client.is_available():
        if FALLBACK_ONLY and LLM_PROVIDER == "hosted":
            logger.warning("Hosted LLM unavailable; starting in fallback-only mode.")
        else:
            if LLM_PROVIDER == "ollama":
                logger.error(f"Ollama service not available at {OLLAMA_BASE_URL}. Exiting.")
            else:
                logger.error("Hosted LLM service is not available. Check HOSTED_LLM_URL and HOSTED_LLM_API_KEY.")
            sys.exit(1)

    if LLM_PROVIDER == "ollama":
        logger.info(f"Ollama service available. Using model: {OLLAMA_MODEL}")
    else:
        if not HOSTED_LLM_URL and not FALLBACK_ONLY:
            logger.error("HOSTED_LLM_URL is not configured. Set HOSTED_LLM_URL and HOSTED_LLM_API_KEY.")
            sys.exit(1)
        logger.info(f"Using hosted LLM provider: {HOSTED_LLM_URL or 'fallback-only mode'}")

    await pg_client.connect()

    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        auto_offset_reset="latest",
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
        await llm_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("LLM worker execution interrupted by user.")
