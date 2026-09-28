"""
LLM Intelligence Worker

Reads parsed content from Kafka (crawl.parsed topic), performs LLM analysis
using a hosted or local LLM provider, and writes intelligence analytics to ClickHouse.

Pipeline:
  crawl.parsed (Kafka) → [LLM Worker] → intelligence_analytics + intelligence_entities (ClickHouse)
                          ↓
                      PostgreSQL: update parsed_items.intelligence_processed = true

Reliability guarantees (crash-safe consumption):
  - Kafka auto-commit is DISABLED. Offsets are committed only after a message
    has been fully processed (or permanently failed), inline under the
    concurrency semaphore — a crash can no longer lose in-flight items.
  - Items are idempotent: intelligence_analytics / intelligence_entities are
    ReplacingMergeTree tables keyed on item_id, and the worker skips items
    already flagged intelligence_processed in PostgreSQL (re-delivery safe).
  - Transient LLM failures are retried with exponential backoff; after the
    final attempt the item is skipped (intelligence_processed stays false so
    it can be re-analyzed later) — no extra Kafka topic required.

Intelligence upgrades:
  - RAG context excludes the current item and applies a cosine score threshold
  - Category anchor vectors ground classification in reference texts instead
    of the system's own previously scraped pages
  - The LLM reports a confidence (0-1) stored as llm_score
  - Per-item prompt + RAG context snapshots are stored to MinIO for audit
  - Entities are normalized into a queryable intelligence_entities table
"""

import asyncio
import json
import logging
import os
import re
import signal
import sys
from datetime import datetime
from typing import Optional

import clickhouse_connect
import httpx
from aiokafka import AIOKafkaConsumer

try:
    from groq import Groq
except Exception:  # pragma: no cover - optional dependency fallback
    Groq = None

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
# workers/ package (health + metrics servers) resolves from the project root,
# but keep an explicit entry so running this file from anywhere works.
_WORKERS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _WORKERS_DIR not in sys.path:
    sys.path.append(_WORKERS_DIR)

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.common.constants.intelligence_categories import (
    ALL_INTELLIGENCE_CATEGORIES,
    CATEGORY_DESCRIPTIONS,
    DEFAULT_INTELLIGENCE_CATEGORY,
    get_category_anchor_texts,
)
from app.common.constants.source_types import ALL_SOURCE_TYPES, DEFAULT_SOURCE_TYPE
from app.pipeline.schemas import IntelligenceAnalytics, ParsedItem
from app.storage.postgres.client import pg_client
from workers.common import build_unique_crawl_object_name as _shared_build_unique_crawl_object_name

# The llm-worker directory name contains a dash, so it cannot be imported as
# a package; add it to sys.path so `embedding_rag` resolves both when this
# file is run as a script and when it is loaded as a module (unit tests).
_LLM_WORKER_DIR = os.path.dirname(os.path.abspath(__file__))
if _LLM_WORKER_DIR not in sys.path:
    sys.path.append(_LLM_WORKER_DIR)

from embedding_rag import (  # noqa: E402
    EmbeddingClient,
    VectorDBClient,
    format_similar_articles_context,
)


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

# --- Retry configuration (no new Kafka topic: retries are in-memory) ---
LLM_RETRY_ATTEMPTS = int(os.getenv("LLM_RETRY_ATTEMPTS", "3"))
LLM_RETRY_BACKOFF_SECONDS = float(os.getenv("LLM_RETRY_BACKOFF_SECONDS", "2"))

# --- RAG / Embedding Configuration (Qdrant + Ollama embeddings) ---
RAG_ENABLED = os.getenv("RAG_ENABLED", "false").strip().lower() in ("1", "true", "yes")
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "ollama").strip().lower()
EMBEDDING_BASE_URL = os.getenv("EMBEDDING_BASE_URL", "http://ollama-embedding:11434")
# bge-m3 is multilingual (Amharic + English in one semantic space) and produces
# 1024-dim vectors. EMBEDDING_VECTOR_SIZE must match the model.
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "bge-m3")
EMBEDDING_VECTOR_SIZE = int(os.getenv("EMBEDDING_VECTOR_SIZE", "1024"))
VECTOR_DB_URL = os.getenv("VECTOR_DB_URL", "http://qdrant:6333")
VECTOR_DB_COLLECTION = os.getenv("VECTOR_DB_COLLECTION", "duka_articles")
RAG_TOP_K = int(os.getenv("RAG_TOP_K", "5"))
# Cosine similarity floor: weaker matches never reach the LLM prompt.
RAG_SCORE_THRESHOLD = float(os.getenv("RAG_SCORE_THRESHOLD", "0.35"))
# Minimum cosine similarity for a category anchor hint to be included.
ANCHOR_MIN_SIMILARITY = float(os.getenv("RAG_ANCHOR_MIN_SIMILARITY", "0.5"))
llm_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# --- Prompt audit snapshots (MinIO) ---
PROMPT_AUDIT_ENABLED = os.getenv("PROMPT_AUDIT_ENABLED", "true").strip().lower() in (
    "1", "true", "yes",
)

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

    async def extract_intelligence(
        self, parsed_text: str, url: str, rag_context: str = "", category_hints: str = ""
    ) -> dict:
        """
        Use LLM to extract actionable intelligence from parsed content.

        Args:
            parsed_text: Clean extracted text from parsing phase
            url: Source URL
            rag_context: Optional similar-article context block from Qdrant
            category_hints: Optional anchor-similarity hints for grounding

        Returns:
            Dictionary with keys: category, threat_severity, entities, summary,
            confidence
        """
        if not parsed_text or len(parsed_text.strip()) < 50:
            logger.warning(f"Parsed text too short for analysis: {len(parsed_text)} chars")
            return self._empty_intelligence()

        # Construct prompt for LLM
        category_list_str = "\n".join(
            f'  - "{cat}": {desc}' for cat, desc in CATEGORY_DESCRIPTIONS.items()
        )
        rag_block = f"\n{rag_context.strip()}\n" if rag_context else ""
        hints_block = f"\n{category_hints.strip()}\n" if category_hints else ""
        prompt = f"""Analyze this scraped content for actionable intelligence.

Source URL: {url}

Content:
{parsed_text[:2000]}
{rag_block}{hints_block}
Classify into exactly ONE of these categories:
{category_list_str}

Please respond ONLY with a valid JSON object (no markdown, no explanation) containing:
{{
  "category": one of {sorted(ALL_INTELLIGENCE_CATEGORIES)},
  "threat_severity": integer from 1-5 (1=low, 5=critical),
  "entities": list of extracted entities (names, emails, IPs, domains, sensitive info),
  "summary": brief summary of findings (max 200 chars),
  "confidence": float between 0 and 1 reflecting your certainty in the category classification
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

        # Confidence: clamp into [0, 1]; 0.5 when the model did not report one.
        try:
            confidence = float(data.get("confidence", 0.5))
        except (TypeError, ValueError):
            confidence = 0.5
        confidence = min(max(confidence, 0.0), 1.0)

        return {
            "source_type": source_type,
            "category": category,
            "threat_severity": threat_severity,
            "entities": entities[:100],
            "summary": summary,
            "confidence": confidence,
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

    async def extract_intelligence(
        self, parsed_text: str, url: str, rag_context: str = "", category_hints: str = ""
    ) -> dict:
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
        rag_block = f"\n{rag_context.strip()}\n" if rag_context else ""
        hints_block = f"\n{category_hints.strip()}\n" if category_hints else ""
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
{rag_block}{hints_block}
Classify into exactly ONE intelligence category:
{category_list_str}

Also classify the source type as one of: {source_type_values}

Return ONLY valid JSON with this exact structure:
{{
  "source_type": one of [{source_type_values}],
  "category": one of {sorted(ALL_INTELLIGENCE_CATEGORIES)},
  "threat_severity": integer from 1-5 (1=low, 5=critical),
  "entities": list of extracted entities (names, emails, IPs, domains, sensitive info, locations),
  "summary": a concise 2-4 sentence summary in {summary_language} of the main idea, most important facts, and why it matters,
  "confidence": a float between 0 and 1 reflecting your certainty in the category classification
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
                intelligence["llm_score"] = intelligence["confidence"]
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

# RAG clients (embedding + vector store). Construction is cheap; availability
# is probed lazily per call so a missing Ollama / Qdrant degrades RAG
# gracefully without blocking the worker.
embedding_client = EmbeddingClient(
    base_url=EMBEDDING_BASE_URL,
    model=EMBEDDING_MODEL,
    redis_url=settings.REDIS_URL,
)
vector_db_client = VectorDBClient(base_url=VECTOR_DB_URL, collection_name=VECTOR_DB_COLLECTION)

# Dedicated client for category anchor embeddings (kept separate so article
# embedding calls and anchor warm-up never contend on one httpx pool).
anchor_client = EmbeddingClient(base_url=EMBEDDING_BASE_URL, model=EMBEDDING_MODEL)

# Category anchor vectors, precomputed once at worker startup. These ground
# classification in curated reference texts instead of the system's own
# previously scraped pages (fixes the self-referential RAG loop).
_category_anchor_vectors: dict[str, list[float]] = {}


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two equal-length vectors (0.0 on mismatch)."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


async def _precompute_category_anchors() -> None:
    """Embed each intelligence category's anchor text once (non-fatal)."""
    global _category_anchor_vectors
    if not RAG_ENABLED:
        return
    try:
        if not await anchor_client.is_available():
            logger.warning("RAG: embedding service unavailable; category anchors disabled")
            return
        try:
            await anchor_client.pull_model()
        except Exception:
            pass
        vectors: dict[str, list[float]] = {}
        for category, text in get_category_anchor_texts().items():
            vector = await anchor_client.embed(text)
            if vector:
                vectors[category] = vector
        _category_anchor_vectors = vectors
        logger.info("RAG: precomputed %d category anchor vector(s)", len(vectors))
    except Exception as exc:
        logger.warning("RAG anchor precompute failed (non-fatal): %s", exc)


def _format_category_hints(anchor_hits: list[tuple[str, float]]) -> str:
    """Format anchor-similarity hits as a prompt hint block."""
    if not anchor_hits:
        return ""
    lines = ["CONTENT-BASED CATEGORY HINTS (similarity to reference anchors, supporting evidence only):"]
    for category, score in anchor_hits[:2]:
        lines.append(f"- {category} (similarity {score:.2f})")
    lines.append("Treat these hints as evidence to weigh, not as the final answer.")
    return "\n".join(lines)


# ============================================================================
# RAG (EMBEDDINGS + QDRANT)
# ============================================================================


async def rag_get_context(
    parsed_text: str, language: str = "en", exclude_item_id: str | None = None
) -> tuple[str, Optional[list], list[tuple[str, float]]]:
    """Retrieve similar-article context from Qdrant for the LLM prompt.

    Embeds the parsed text, searches Qdrant for similar stored articles, and
    formats them as a context block. Returns ``(context, embedding, anchor_hits)``
    where context is "" when RAG is disabled, services are unavailable, or
    nothing similar is found. The embedding is returned alongside so it can be
    reused for the upsert (one embed call per article). Never raises.

    The current item is always excluded (a re-analyzed document must never
    retrieve itself as context) and results below RAG_SCORE_THRESHOLD are
    dropped. anchor_hits are cosine similarities to the category anchor texts.
    """
    if not RAG_ENABLED or not parsed_text or len(parsed_text.strip()) < 50:
        return "", None, []

    try:
        if not await embedding_client.is_available():
            logger.warning("RAG: embedding service unavailable; skipping context retrieval")
            return "", None, []
        embedding = await embedding_client.embed(parsed_text)
        if not embedding:
            return "", None, []

        anchor_hits: list[tuple[str, float]] = []
        if _category_anchor_vectors:
            for category, vec in _category_anchor_vectors.items():
                score = _cosine_similarity(embedding, vec)
                if score >= ANCHOR_MIN_SIMILARITY:
                    anchor_hits.append((category, score))
            anchor_hits.sort(key=lambda kv: kv[1], reverse=True)

        if not await vector_db_client.is_available():
            logger.warning("RAG: vector DB unavailable; skipping context retrieval")
            return "", embedding, anchor_hits
        language_filter = language if language not in ("", "unknown", "und") else None
        similar = await vector_db_client.search_similar(
            embedding,
            top_k=RAG_TOP_K,
            language_filter=language_filter,
            exclude_item_id=exclude_item_id,
            score_threshold=RAG_SCORE_THRESHOLD,
        )
        context = format_similar_articles_context(similar) if similar else ""
        if context:
            logger.info(f"RAG: found {len(similar)} similar article(s) as context")
        return context, embedding, anchor_hits
    except Exception as e:
        logger.warning(f"RAG context retrieval failed: {e}")
        return "", None, []


async def rag_store_embedding(
    item_id: str,
    url: str,
    text: str,
    language: str = "en",
    category: str = "",
    text_summary: str = "",
    embedding: Optional[list] = None,
    job_id: str = "",
) -> bool:
    """Embed a parsed article and upsert it into Qdrant.

    Returns True on success; never raises. The stored payload carries the LLM
    category and summary so future similar-article lookups can display them.
    Pass ``embedding`` from :func:`rag_get_context` to avoid re-embedding.
    """
    if not RAG_ENABLED or not text or len(text.strip()) < 10:
        return False

    try:
        if not await embedding_client.is_available():
            logger.warning("RAG: embedding service unavailable; skipping upsert")
            return False
        if embedding is None:
            embedding = await embedding_client.embed(text)
        if not embedding:
            return False
        if not await vector_db_client.is_available():
            logger.warning("RAG: vector DB unavailable; skipping upsert")
            return False
        stored = await vector_db_client.store_embedding(
            item_id=item_id,
            embedding=embedding,
            vector_size=EMBEDDING_VECTOR_SIZE,
            metadata={
                "url": url,
                "job_id": job_id,
                "language": language,
                "category": category,
                "text_summary": text_summary,
            },
        )
        if stored:
            logger.info(f"RAG: stored embedding for item {item_id}")
        return stored
    except Exception as e:
        logger.warning(f"RAG embedding storage failed: {e}")
        return False


# ============================================================================
# PROMPT AUDIT SNAPSHOTS (MinIO)
# ============================================================================


_minio_client = None
_minio_ready = False


def _get_minio_client():
    """Lazy MinIO client for prompt audit snapshots.

    Never raises and never hangs: every socket operation is bounded by a
    short urllib3 timeout so a wedged MinIO costs a few seconds, not a
    stuck thread in the worker's thread pool.
    """
    global _minio_client, _minio_ready
    if _minio_ready:
        return _minio_client
    _minio_ready = True
    try:
        import urllib3
        from minio import Minio

        _minio_client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=settings.MINIO_SECURE,
            http_client=urllib3.PoolManager(
                timeout=urllib3.Timeout(connect=2.0, read=5.0),
            ),
        )
        if not _minio_client.bucket_exists(settings.MINIO_PARSED_BUCKET):
            _minio_client.make_bucket(settings.MINIO_PARSED_BUCKET)
    except Exception as exc:
        logger.debug("Prompt audit: MinIO unavailable (snapshots disabled): %s", exc)
        _minio_client = None
    return _minio_client


async def store_prompt_snapshot(
    job_id: str,
    item_id: str,
    url: str,
    model: str,
    rag_context: str,
    category_hints: str,
    intelligence: dict,
) -> None:
    """Persist the exact RAG context + hints used for one classification.

    Written next to the parsed JSON in duka-parsed-data so any classification
    can be audited/debugged later. Best-effort: failures never block the
    pipeline.
    """
    if not PROMPT_AUDIT_ENABLED:
        return
    try:
        client = await asyncio.to_thread(_get_minio_client)
        if client is None:
            return
        snapshot = {
            "item_id": item_id,
            "job_id": job_id,
            "url": url,
            "llm_model": model,
            "analyzed_at": datetime.now().isoformat(),
            "rag_context": rag_context,
            "category_hints": category_hints,
            "result": {
                "source_type": intelligence.get("source_type"),
                "category": intelligence.get("category"),
                "threat_severity": intelligence.get("threat_severity"),
                "entities": intelligence.get("entities"),
                "summary": intelligence.get("summary"),
                "confidence": intelligence.get("confidence"),
            },
        }
        object_name = f"{job_id}/{item_id}_llm_context.json"
        payload = json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
        from io import BytesIO

        await asyncio.to_thread(
            client.put_object,
            settings.MINIO_PARSED_BUCKET,
            object_name,
            BytesIO(payload),
            len(payload),
            "application/json",
        )
        logger.debug("Prompt audit snapshot stored: %s", object_name)
    except Exception as exc:
        logger.debug("Prompt audit snapshot failed (non-fatal): %s", exc)


# ============================================================================
# CLICKHOUSE INGEST
# ============================================================================


async def ingest_intelligence_to_clickhouse(
    intelligence: IntelligenceAnalytics,
    entities_by_type: Optional[dict[str, list[str]]] = None,
):
    """
    Insert intelligence record into ClickHouse intelligence_analytics and
    flatten entities into intelligence_entities.

    Both tables are ReplacingMergeTree keyed on item_id/entity+item_id, so
    at-least-once Kafka re-delivery cannot produce duplicate analytics.

    Args:
        intelligence: IntelligenceAnalytics object
        entities_by_type: optional mapping of entity_type -> entity values
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

                # Flattened entity rows for "every page mentioning X" queries.
                if entities_by_type:
                    entity_rows = []
                    for entity_type, values in entities_by_type.items():
                        for value in values:
                            entity_rows.append([
                                intelligence.item_id,
                                f"{entity_type}:{value}",
                                intelligence.job_id,
                                intelligence.url,
                                intelligence.category,
                                intelligence.language,
                                intelligence.threat_severity,
                                intelligence.created_at,
                            ])
                    if entity_rows:
                        client.insert(
                            "intelligence_entities",
                            entity_rows,
                            column_names=[
                                "item_id", "entity", "job_id", "url", "category",
                                "language", "threat_severity", "created_at",
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


async def _pg_call(coro_factory, *args, default=None, **kwargs):
    """Await a pg_client call, degrading gracefully when it is unavailable.

    Mock-safe: in unit tests pg_client is partially mocked, and the mock
    attributes that are not AsyncMock raise on await — treat that the same
    as a real outage (non-fatal default).
    """
    try:
        return await coro_factory(*args, **kwargs)
    except Exception as exc:
        logger.debug("PostgreSQL call failed (non-fatal): %s", exc)
        return default


async def is_intelligence_processed(item_id: str) -> bool:
    """True when the item was already analyzed (Kafka re-delivery guard).

    Degrades to False (process anyway) when PostgreSQL is unreachable —
    ClickHouse idempotency (ReplacingMergeTree) covers the overlap.
    """
    row = await _pg_call(pg_client.get_parsed_item, item_id, default=None)
    if isinstance(row, dict):
        return bool(row.get("intelligence_processed"))
    return False


def _looks_transient(exc: Exception) -> bool:
    """Heuristic: retry LLM calls on transient/network-style failures."""
    text = f"{type(exc).__name__}: {exc}".lower()
    markers = (
        "timeout", "timed out", "connection", "temporarily", "rate limit",
        "429", "502", "503", "504", "unavailable", "reset",
    )
    return any(marker in text for marker in markers)


async def _analyze_with_retry(parsed_text: str, url: str, rag_context: str, category_hints: str) -> dict:
    """LLM analysis with exponential backoff on transient failures.

    After the final attempt the item is skipped (intelligence_processed stays
    false so it can be re-analyzed later) — no extra Kafka topic required.
    """
    last_error: Exception | None = None
    for attempt in range(1, LLM_RETRY_ATTEMPTS + 1):
        try:
            return await llm_client.extract_intelligence(
                parsed_text, url, rag_context=rag_context, category_hints=category_hints
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            if not _looks_transient(exc) or attempt == LLM_RETRY_ATTEMPTS:
                break
            delay = LLM_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Transient LLM failure (attempt %d/%d) for %s: %s — retrying in %.1fs",
                attempt, LLM_RETRY_ATTEMPTS, url, exc, delay,
            )
            await asyncio.sleep(delay)
    logger.error(
        "LLM analysis failed permanently for %s after %d attempt(s): %s",
        url, LLM_RETRY_ATTEMPTS, last_error,
    )
    raise last_error if last_error else RuntimeError("LLM analysis failed")


async def process_parsed_item(message_value: bytes):
    """
    Process a parsed item from Kafka, perform LLM analysis, and store results.

    Idempotent: items already flagged intelligence_processed are skipped, and
    all writes are re-delivery-safe (ReplacingMergeTree + deterministic point
    ids). Args:
        message_value: Raw Kafka message value (JSON)
    """
    try:
        data = json.loads(message_value)
        parsed_item = ParsedItem(**data)

        logger.info(f"Processing parsed item: job={parsed_item.job_id} item={parsed_item.item_id} url={parsed_item.url}")

        # Skip items the pipeline already flagged as failures — they are not
        # worth an LLM call (language-rejected content is handled upstream too).
        if parsed_item.status == "failed":
            logger.info(f"Skipping failed item {parsed_item.item_id}")
            return

        # Idempotency pre-check: re-delivery after a crash must not re-analyze
        # (and must not duplicate ClickHouse rows).
        if await is_intelligence_processed(parsed_item.item_id):
            logger.info(f"Item {parsed_item.item_id} already intelligence_processed — skipping")
            return

        # Extract text from parsed data
        if isinstance(parsed_item.data, dict):
            data_dict = parsed_item.data
        else:
            data_dict = parsed_item.data.model_dump()
        extracted_text = data_dict.get("extracted_text", "")

        # RAG: retrieve similar articles from Qdrant to inject as context into
        # the LLM prompt (no-op when RAG_ENABLED is false or services are down).
        # The current item is excluded from its own context, and only matches
        # above the similarity threshold are kept.
        rag_context = ""
        rag_embedding = None
        anchor_hits: list[tuple[str, float]] = []
        if RAG_ENABLED:
            rag_context, rag_embedding, anchor_hits = await rag_get_context(
                extracted_text, parsed_item.language, exclude_item_id=parsed_item.item_id
            )
        category_hints = _format_category_hints(anchor_hits)

        # Perform LLM analysis (with RAG context when available). Retries with
        # backoff on transient errors; raises after the final attempt.
        try:
            intelligence_data = await _analyze_with_retry(
                extracted_text, parsed_item.url, rag_context, category_hints
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return  # permanently failed: stays intelligence_processed=false

        llm_model = HOSTED_LLM_MODEL if LLM_PROVIDER != "ollama" else OLLAMA_MODEL

        # Clamp severity at the call site: the LLM path can yield values the
        # schema would otherwise silently accept (0 or negatives are invalid
        # per the 1-5 contract). _validate_intelligence already clamps; this is
        # defense in depth for raw dict passthroughs.
        try:
            severity = int(intelligence_data.get("threat_severity", 1))
        except (TypeError, ValueError):
            severity = 1
        severity = min(max(severity, 1), 5)

        # Confidence (0-1) is the real per-response certainty signal; fall back
        # to llm_score for legacy providers that do not report confidence.
        confidence = intelligence_data.get("confidence")
        if confidence is None:
            confidence = intelligence_data.get("llm_score", 0.0)

        # Create intelligence analytics record
        intelligence = IntelligenceAnalytics(
            job_id=parsed_item.job_id,
            item_id=parsed_item.item_id,
            url=parsed_item.url,
            source_type=intelligence_data.get("source_type", DEFAULT_SOURCE_TYPE),
            category=intelligence_data.get("category", DEFAULT_INTELLIGENCE_CATEGORY),
            threat_severity=severity,
            entities=intelligence_data.get("entities", []),
            summary=intelligence_data.get("summary", ""),
            language=parsed_item.language,
            llm_model=llm_model,
            llm_score=confidence,
        )

        # Typed entities for the intelligence_entities table: emails, IPs,
        # domains, everything else. Best-effort — regex only, never fails.
        entities_by_type = _typed_entities(intelligence.entities)

        # Ingest to ClickHouse (analytics + flattened entity rows)
        await ingest_intelligence_to_clickhouse(intelligence, entities_by_type)

        # Audit snapshot: exact RAG context + hints used for this classification.
        await store_prompt_snapshot(
            job_id=parsed_item.job_id,
            item_id=parsed_item.item_id,
            url=parsed_item.url,
            model=llm_model,
            rag_context=rag_context,
            category_hints=category_hints,
            intelligence=intelligence_data,
        )

        # RAG: embed and upsert this article into Qdrant for future retrieval.
        # The embedding produced during context retrieval is reused here.
        if RAG_ENABLED:
            await rag_store_embedding(
                item_id=parsed_item.item_id,
                url=parsed_item.url,
                text=extracted_text,
                language=parsed_item.language,
                category=intelligence.category,
                text_summary=intelligence.summary,
                embedding=rag_embedding,
                job_id=parsed_item.job_id,
            )

        # Update PostgreSQL: mark as intelligence_processed
        await _pg_call(pg_client.mark_item_intelligence_processed, parsed_item.item_id)
        logger.debug(f"Updated PostgreSQL: item {parsed_item.item_id} marked as intelligence_processed")

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing parsed item: {e}", exc_info=True)


def _typed_entities(entities: list[str]) -> dict[str, list[str]]:
    """Best-effort entity typing (email/ip/domain/other) for the entity table."""
    typed: dict[str, list[str]] = {"email": [], "ip": [], "domain": [], "other": []}
    for entity in entities or []:
        value = str(entity).strip()
        if not value:
            continue
        if "@" in value and "." in value.split("@")[-1]:
            typed["email"].append(value)
        elif re.fullmatch(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", value):
            typed["ip"].append(value)
        elif re.fullmatch(r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}", value):
            typed["domain"].append(value)
        else:
            typed["other"].append(value)
    return {k: v for k, v in typed.items() if v}


async def process_message_safely(message_value: bytes):
    async with llm_semaphore:
        await process_parsed_item(message_value)


async def main():
    """Main LLM worker lifecycle loop."""
    from workers.health import HealthServer
    from workers.metrics import WorkerMetrics

    health = HealthServer(worker_name="llm")
    await health.start()
    metrics = WorkerMetrics(worker_name="llm", topic=CONSUME_TOPIC)
    await metrics.start()

    if not await llm_client.is_available():
        if FALLBACK_ONLY and LLM_PROVIDER == "hosted":
            logger.warning("Hosted LLM unavailable; starting in fallback-only mode.")
        else:
            if LLM_PROVIDER == "ollama":
                logger.error(f"Ollama service not available at {OLLAMA_BASE_URL}. Exiting.")
            else:
                logger.error("Hosted LLM service is not available. Check HOSTED_LLM_URL and HOSTED_LLM_API_KEY.")
            await health.stop()
            await metrics.stop()
            sys.exit(1)

    if LLM_PROVIDER == "ollama":
        logger.info(f"Ollama service available. Using model: {OLLAMA_MODEL}")
    else:
        if not HOSTED_LLM_URL and not FALLBACK_ONLY:
            logger.error("HOSTED_LLM_URL is not configured. Set HOSTED_LLM_URL and HOSTED_LLM_API_KEY.")
            await health.stop()
            await metrics.stop()
            sys.exit(1)
        logger.info(f"Using hosted LLM provider: {HOSTED_LLM_URL or 'fallback-only mode'}")

    if RAG_ENABLED:
        logger.info("RAG enabled: probing embedding + vector DB services...")
        await _precompute_category_anchors()
        if await embedding_client.is_available():
            logger.info(f"RAG embedding service available: {EMBEDDING_MODEL} @ {EMBEDDING_BASE_URL}")
        else:
            logger.warning(
                f"RAG: embedding service unavailable at {EMBEDDING_BASE_URL}; RAG will degrade gracefully"
            )
        if await vector_db_client.is_available():
            logger.info(f"RAG vector DB available: {VECTOR_DB_COLLECTION} @ {VECTOR_DB_URL}")
        else:
            logger.warning(
                f"RAG: vector DB unavailable at {VECTOR_DB_URL}; RAG will degrade gracefully"
            )

    await pg_client.connect()

    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        # 'earliest' so messages published while this worker was down are still
        # consumed after a restart (idempotency pre-check + ReplacingMergeTree
        # make replays safe; 'latest' would silently skip them).
        auto_offset_reset="earliest",
        # Crash-safe consumption: offsets are committed explicitly after each
        # message finishes processing (inline under the semaphore), so a crash
        # can no longer lose in-flight items the way auto-commit could.
        enable_auto_commit=False,
    )

    await consumer.start()
    health.mark_ready()
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

    async def _commit(consumer: AIOKafkaConsumer) -> None:
        try:
            await consumer.commit()
        except Exception as exc:
            logger.warning("Offset commit failed (will retry after next message): %s", exc)

    try:
        # Inline processing: one message at a time under the concurrency
        # semaphore. Combined with manual commits this gives at-least-once
        # semantics with no data-loss window between poll and commit.
        async for msg in consumer:
            metrics.record_consumed()
            async with llm_semaphore:
                try:
                    await process_parsed_item(msg.value)
                    metrics.record_processed("success")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Permanent failure after retries: log + continue so one
                    # poison item cannot stall the pipeline. The item keeps
                    # intelligence_processed=false and can be re-analyzed later.
                    metrics.record_error("processing_error")
                    metrics.record_processed("error")
                    logger.error("Message processing failed after retries: %s", exc, exc_info=True)
                # Commit regardless of processing outcome: forward progress is
                # preserved and unprocessable items are not redelivered forever.
                await _commit(consumer)
    except asyncio.CancelledError:
        logger.info("LLM worker cancellation requested.")
    finally:
        logger.info("Shutting down LLM worker gracefully...")
        await consumer.stop()
        await pg_client.close()
        await llm_client.close()
        await embedding_client.close()
        await vector_db_client.close()
        await anchor_client.close()
        await metrics.stop()
        await health.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("LLM worker execution interrupted by user.")
