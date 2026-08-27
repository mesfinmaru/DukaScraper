"""
LLM Intelligence Worker with RAG Integration

Reads parsed content from Kafka (crawl.parsed topic), performs LLM analysis
using hosted LLM provider with RAG context, and writes intelligence analytics to ClickHouse.

Pipeline:
  crawl.parsed (Kafka) → [Embedding (Ollama)] → [Search Qdrant] → [RAG Context]
                          → [LLM with Context (Groq)] → [ClickHouse] → intelligence_analytics
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



# --- Import RAG modules ---
try:
    from embedding_rag import EmbeddingClient, VectorDBClient, format_similar_articles_context
    RAG_AVAILABLE = True
except ImportError:
    RAG_AVAILABLE = False

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
from app.common.constants.content_topics import ALL_CONTENT_TOPICS, DEFAULT_CONTENT_TOPIC, TOPIC_DESCRIPTIONS
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

# --- LLM Configuration ---
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "hosted").strip().lower()
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2:8b")
HOSTED_LLM_URL = os.getenv("HOSTED_LLM_URL", "")
HOSTED_LLM_API_KEY = os.getenv("HOSTED_LLM_API_KEY", "")
HOSTED_LLM_MODEL = os.getenv("HOSTED_LLM_MODEL", "google-gemini-flash")
FALLBACK_ONLY = os.getenv("FALLBACK_ONLY", "false").strip().lower() in ("1", "true", "yes")
MAX_CONCURRENT_TASKS = settings.LLM_MAX_CONCURRENT_TASKS
llm_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# --- RAG Configuration ---
RAG_ENABLED_ENV = os.getenv("RAG_ENABLED", "true").strip().lower() in ("1", "true", "yes")
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "ollama")
EMBEDDING_BASE_URL = os.getenv("EMBEDDING_BASE_URL", "http://ollama-embedding:11434")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
VECTOR_DB_URL = os.getenv("VECTOR_DB_URL", "http://qdrant:6333")
VECTOR_DB_COLLECTION = os.getenv("VECTOR_DB_COLLECTION", "duka_articles")

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
# HOSTED LLM CLIENT WITH RAG INTEGRATION
# ============================================================================


class HostedLLMClient:
    """Hosted LLM client with RAG integration for context-aware intelligence extraction."""

    def __init__(self, base_url: str, api_key: str = "", model: str = ""):
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.api_key = api_key
        self.model = model
        self.provider = self._detect_provider(self.base_url)
        self.client = httpx.AsyncClient(timeout=120)
        self.groq_client = None
        if self.provider == "groq" and api_key and Groq is not None:
            self.groq_client = Groq(api_key=api_key)
        
        # Initialize RAG clients if enabled
        self.embedding_client = None
        self.vector_db_client = None
        if RAG_AVAILABLE and RAG_ENABLED_ENV:
            self.embedding_client = EmbeddingClient(EMBEDDING_BASE_URL, EMBEDDING_MODEL)
            self.vector_db_client = VectorDBClient(VECTOR_DB_URL, VECTOR_DB_COLLECTION)
            logger.info("RAG clients initialized")

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
        if self.embedding_client:
            await self.embedding_client.close()
        if self.vector_db_client:
            await self.vector_db_client.close()

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
            return bool(self.api_key)
        if "groq.com" in self.base_url.lower():
            return bool(self.api_key)

        try:
            response = await self.client.head(self.base_url, timeout=5)
            return response.status_code in (200, 204, 301, 302)
        except Exception:
            return False

    async def extract_intelligence(self, parsed_text: str, url: str) -> dict:
        """Extract intelligence with optional RAG context."""
        if not parsed_text or len(parsed_text.strip()) < 50:
            return {
                "source_type": DEFAULT_SOURCE_TYPE,
                "category": DEFAULT_INTELLIGENCE_CATEGORY,
                "threat_severity": 1,
                "entities": [],
                "summary": "Unable to analyze reliably; text too short.",
                "llm_score": 0.0,
            }

        if not self.api_key and FALLBACK_ONLY:
            logger.warning("No hosted API key available; using fallback analyzer.")
            return self._fallback_analyze(parsed_text=parsed_text, url=url)

        category_list_str = "\n".join(
            f'  - "{cat}": {desc}' for cat, desc in CATEGORY_DESCRIPTIONS.items()
        )
        source_type_values = "news, forum, blog, social, government, academic, encyclopedia, ecommerce, other"
        topic_list_str = "\n".join(f'  - "{topic}": {description}' for topic, description in TOPIC_DESCRIPTIONS.items())
        content_language = HostedLLMClient._detect_content_language(parsed_text)
        summary_language = "Amharic" if content_language == "am" else "English"
        max_analysis_chars = 2500
        if len(parsed_text) <= max_analysis_chars:
            full_content = parsed_text
        else:
            half = max_analysis_chars // 2
            full_content = f"{parsed_text[:half]}\n\n[...middle omitted for provider limit...]\n\n{parsed_text[-half:]}"
        
        # --- RAG: Get Similar Articles Context ---
        rag_context = ""
        if self.embedding_client and self.vector_db_client:
            try:
                # Generate embedding for current text
                embedding = await self.embedding_client.embed(parsed_text)
                if embedding:
                    # Search for similar articles in Qdrant
                    similar_articles = await self.vector_db_client.search_similar(
                        embedding=embedding,
                        top_k=5,
                        language_filter=content_language
                    )
                    if similar_articles:
                        # Keep retrieval useful without allowing prior article
                        # text to consume the provider's token budget.
                        rag_context = format_similar_articles_context(similar_articles)[:2000]
                        logger.info(f"RAG: Found {len(similar_articles)} similar articles for context enhancement")
            except Exception as e:
                logger.warning(f"RAG context retrieval failed: {e}; continuing without context")
        
        prompt = f"""You are a careful content analyst. Read the full content below, not just the headline or first paragraph.

Source URL: {url}

{rag_context}

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

Also classify the subject topic as exactly one of:
{topic_list_str}

Return ONLY valid JSON with this exact structure:
{{
  "source_type": one of [{source_type_values}],
  "topic": one of {sorted(ALL_CONTENT_TOPICS)},
  "category": one of {sorted(ALL_INTELLIGENCE_CATEGORIES)},
  "threat_severity": integer from 1-5 (1=low, 5=critical),
  "entities": list of at most 15 extracted entities (names, organizations, emails, IPs, domains, locations),
  "summary": a concise summary in {summary_language}, no more than 500 characters
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
                "max_completion_tokens": 2048,
                "top_p": 1,
                "response_format": {"type": "json_object"},
                "stream": False,
                "stop": None,
            }
        else:
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            body = {"model": self.model or OLLAMA_MODEL, "prompt": prompt}

        try:
            if self.provider == "groq" and self.groq_client is not None:
                # The Groq SDK is synchronous. Running it in a worker thread
                # prevents API retries from blocking Kafka heartbeats and every
                # other item in this async consumer.
                completion = await asyncio.to_thread(
                    self.groq_client.chat.completions.create,
                    model=self.model or "openai/gpt-oss-120b",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_completion_tokens=1024,
                    top_p=1,
                    response_format={"type": "json_object"},
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
                intelligence = self._parse_intelligence_response(response_text)
                intelligence = self._validate_intelligence(intelligence)
                intelligence["llm_score"] = None
            except Exception:
                intelligence = {
                    "source_type": DEFAULT_SOURCE_TYPE,
                    "topic": DEFAULT_CONTENT_TOPIC,
                    "category": DEFAULT_INTELLIGENCE_CATEGORY,
                    "threat_severity": 1,
                    "entities": [],
                    "summary": response_text if response_text else "Unable to analyze reliably.",
                "llm_score": None,
                }

            return intelligence
        except Exception as e:
            logger.warning(f"Hosted LLM request failed: {e}")
            return self._fallback_analyze(parsed_text=parsed_text, url=url)

    @staticmethod
    def _parse_intelligence_response(response_text: str) -> dict:
        """Parse LLM response text into structured intelligence."""
        cleaned_text = response_text.strip()
        if cleaned_text.startswith("```"):
            cleaned_text = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned_text, flags=re.IGNORECASE)
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
        """Fallback rule-based analyzer."""
        text = (parsed_text or "").strip()
        summary = (text[:600] + "...") if len(text) > 600 else text

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
        topic = DEFAULT_CONTENT_TOPIC
        if any(k in lowered for k in ("economy", "economic", "inflation", "market", "trade", "finance", "bank")):
            topic = "economics"
        elif any(k in lowered for k in ("policy", "government", "minister", "parliament", "election")):
            topic = "politics"
        elif any(k in lowered for k in ("health", "hospital", "disease", "covid", "vaccine")):
            topic = "health"
        if any(k in lowered for k in ("attack", "bomb", "explosion", "violent", "kill")):
            category = "physical_threat"
        elif any(k in lowered for k in ("policy", "government", "minister", "parliament")):
            category = "gov_issue"
        elif any(k in lowered for k in ("health", "hospital", "disease", "covid", "vaccine")):
            category = DEFAULT_INTELLIGENCE_CATEGORY

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
            "topic": topic,
            "category": category,
            "threat_severity": 1,
            "entities": list(dict.fromkeys([e for e in entities if e])),
            "summary": summary if summary else "No summary available",
            "llm_score": None,
        }

    @staticmethod
    def _validate_intelligence(data: dict) -> dict:
        """Validate and normalize LLM output."""
        category = data.get("category", DEFAULT_INTELLIGENCE_CATEGORY)
        if category not in ALL_INTELLIGENCE_CATEGORIES:
            category = DEFAULT_INTELLIGENCE_CATEGORY

        source_type = str(data.get("source_type") or DEFAULT_SOURCE_TYPE).strip().lower()
        if source_type not in ALL_SOURCE_TYPES:
            source_type = DEFAULT_SOURCE_TYPE

        topic = str(data.get("topic") or DEFAULT_CONTENT_TOPIC).strip().lower()
        if topic not in ALL_CONTENT_TOPICS:
            topic = DEFAULT_CONTENT_TOPIC

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
            "source_type": source_type,
            "topic": topic,
            "category": category,
            "threat_severity": max(1, min(5, int(data.get("threat_severity", 1)))),
            "entities": entities[:100],
            "summary": summary,
        }


# Initialize LLM client
llm_client = HostedLLMClient(HOSTED_LLM_URL, HOSTED_LLM_API_KEY, HOSTED_LLM_MODEL)


# ============================================================================
# CLICKHOUSE INGEST
# ============================================================================


async def ingest_intelligence_to_clickhouse(intelligence: IntelligenceAnalytics):
    """Insert intelligence record into ClickHouse intelligence_analytics table."""
    if not ch_client:
        logger.error("ClickHouse client not available, skipping ingest")
        return

    try:
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
                        intelligence.topic,
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
                        "job_id", "item_id", "url", "source_type", "topic", "category", "threat_severity",
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


async def build_section_map_reduce_input(sections: list, url: str, fallback_text: str) -> str:
    """Summarize section chunks before producing a whole-article analysis."""
    valid_sections = [section for section in sections if isinstance(section, dict) and len(str(section.get("text", "")).strip()) >= 80]
    if len(valid_sections) < 2:
        return fallback_text

    mapped: list[str] = []
    # Bounds provider cost while retaining coverage across a long article.
    for section in valid_sections[:12]:
        heading = str(section.get("heading", "Section")).strip()
        section_text = str(section.get("text", "")).strip()
        section_analysis = await llm_client.extract_intelligence(section_text, url)
        mapped.append(f"[{heading}]\n{section_analysis.get('summary', '')}")
    return "\n\n".join(mapped)


# ============================================================================
# MESSAGE PROCESSING
# ============================================================================


async def process_parsed_item(message_value: bytes):
    """Process a parsed item from Kafka with RAG-enhanced LLM analysis."""
    try:
        data = json.loads(message_value)
        parsed_item = ParsedItem(**data)

        logger.info(f"Processing parsed item: job={parsed_item.job_id} item={parsed_item.item_id} url={parsed_item.url}")

        if isinstance(parsed_item.data, dict):
            extracted_text = parsed_item.data.get("extracted_text", "")
            sections = parsed_item.data.get("sections", [])
        else:
            extracted_text = parsed_item.data.extracted_text
            sections = []

        analysis_text = await build_section_map_reduce_input(
            sections if isinstance(sections, list) else [],
            parsed_item.url,
            extracted_text,
        )
        intelligence_data = await llm_client.extract_intelligence(analysis_text, parsed_item.url)
        llm_model = HOSTED_LLM_MODEL

        intelligence = IntelligenceAnalytics(
            job_id=parsed_item.job_id,
            item_id=parsed_item.item_id,
            url=parsed_item.url,
            source_type=intelligence_data.get("source_type", DEFAULT_SOURCE_TYPE),
            topic=intelligence_data.get("topic", DEFAULT_CONTENT_TOPIC),
            category=intelligence_data.get("category", DEFAULT_INTELLIGENCE_CATEGORY),
            threat_severity=intelligence_data.get("threat_severity", 0),
            entities=intelligence_data.get("entities", []),
            summary=intelligence_data.get("summary", ""),
            language=parsed_item.language,
            llm_model=llm_model,
            llm_score=intelligence_data.get("llm_score", 0.0),
        )

        # Persist the current article only after its analysis metadata is
        # available.  Retrieval above supplies RAG context, while this upsert
        # makes the article available as context for later jobs.
        if llm_client.embedding_client and llm_client.vector_db_client:
            try:
                embedding = await llm_client.embedding_client.embed(extracted_text)
                if embedding:
                    stored = await llm_client.vector_db_client.store_embedding(
                        parsed_item.item_id,
                        embedding,
                        {
                            "url": parsed_item.url,
                            "language": parsed_item.language,
                            "category": intelligence.category,
                            "text_summary": intelligence.summary,
                        },
                    )
                    if stored:
                        logger.info("Stored vector for item %s in Qdrant", parsed_item.item_id)
                    else:
                        logger.warning("Qdrant did not store vector for item %s", parsed_item.item_id)
            except Exception as e:
                logger.warning("Failed to store Qdrant vector for item %s: %s", parsed_item.item_id, e)

        # Store each extracted section independently so RAG retrieves focused
        # evidence instead of a whole long article as one noisy vector.
        if llm_client.embedding_client and llm_client.vector_db_client:
            for index, section in enumerate(sections if isinstance(sections, list) else [], start=1):
                section_text = str(section.get("text", "")).strip() if isinstance(section, dict) else ""
                if len(section_text) < 80:
                    continue
                try:
                    section_embedding = await llm_client.embedding_client.embed(section_text)
                    if section_embedding:
                        heading = str(section.get("heading", "Section"))
                        await llm_client.vector_db_client.store_embedding(
                            f"{parsed_item.item_id}:section:{index}",
                            section_embedding,
                            {
                                "url": parsed_item.url,
                                "language": parsed_item.language,
                                "category": intelligence.category,
                                "text_summary": f"{heading}: {section_text}",
                            },
                        )
                except Exception as e:
                    logger.warning("Failed to store section %s for item %s: %s", index, parsed_item.item_id, e)

        await ingest_intelligence_to_clickhouse(intelligence)

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
    """Main LLM worker lifecycle loop with RAG initialization."""
    if not await llm_client.is_available():
        logger.error("Hosted LLM service is not available. Check HOSTED_LLM_URL and HOSTED_LLM_API_KEY.")
        sys.exit(1)

    logger.info(f"Using hosted LLM provider: {HOSTED_LLM_URL}")
    
    # Initialize RAG if enabled
    if RAG_AVAILABLE and RAG_ENABLED_ENV and llm_client.embedding_client and llm_client.vector_db_client:
        try:
            embedding_available = await llm_client.embedding_client.is_available()
            vector_db_available = await llm_client.vector_db_client.is_available()
            
            if embedding_available and vector_db_available:
                # Create collection if it doesn't exist
                await llm_client.vector_db_client.create_collection()
                # Pull embedding model if needed
                if EMBEDDING_PROVIDER == "ollama":
                    await llm_client.embedding_client.pull_model()
                logger.info("RAG system initialized successfully (Ollama embedding + Qdrant vector DB)")
            else:
                logger.warning("RAG components not fully available; running without RAG context")
        except Exception as e:
            logger.warning(f"RAG initialization failed: {e}; continuing without RAG")
    elif not RAG_ENABLED_ENV:
        logger.info("RAG disabled via environment (RAG_ENABLED=false)")

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
        await llm_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("LLM worker execution interrupted by user.")