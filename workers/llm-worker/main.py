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
import time
from datetime import datetime
from typing import Optional

import clickhouse_connect
import httpx
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter

# Provenance counters. A crawl can "succeed" while every item fell back to the
# rule-based heuristic, which is invisible in the item counts but fatal to
# classification quality - these make it observable in Prometheus.
llm_fallback_items = Counter(
    "duka_llm_fallback_items_total",
    "Parsed items classified without a real LLM response.",
    ["source"],
)
llm_request_errors = Counter(
    "duka_llm_request_errors_total",
    "LLM provider requests that failed, by reason.",
    ["reason"],
)

# Failures that retrying cannot fix. An invalid/expired key or a revoked
# account keeps returning the same answer for every item, so it is reported as
# an operator problem rather than as routine per-item noise.
_PERMANENT_LLM_ERRORS = frozenset({"auth", "permission"})
_PERMANENT_LLM_ERROR_LOG_INTERVAL = 300.0  # seconds
_last_permanent_llm_error_at = 0.0


def _looks_transient(exc: Exception) -> bool:
    """Heuristic: retry LLM calls on transient/network-style failures."""
    if isinstance(exc, LLMResponseTruncated):
        # A truncated answer is a budget problem, not a dead provider; the retry
        # runs with a larger completion limit and usually succeeds.
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    markers = (
        "timeout", "timed out", "connection", "temporarily", "rate limit",
        "429", "502", "503", "504", "unavailable", "reset",
    )
    return any(marker in text for marker in markers)


def _classify_llm_error(exc: Exception) -> str:
    """Bucket a provider exception into a short, actionable reason label.

    Used for metrics and to decide whether the fault is worth escalating, so it
    deliberately keys off the status code the SDK surfaces rather than message
    text, which providers reword freely.
    """
    status = getattr(exc, "status_code", None)
    if status is None:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
    if isinstance(exc, LLMResponseTruncated):
        return "truncated"
    if status in (401, 403):
        return "auth"
    if status == 404:
        return "model_not_found"
    if status == 429:
        return "rate_limited"
    if status is not None and 500 <= int(status) < 600:
        return "provider_error"
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return "timeout"
    name = type(exc).__name__.lower()
    if "connection" in name:
        return "connection"
    return "other"

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
from app.common.utils.minio_naming import site_from_url
from app.pipeline.schemas import IntelligenceAnalytics, ParsedItem
from app.services.alert_service import record_alert
from app.services.groq_batch import GroqBatchClient, GroqBatchError
from app.services.llm_analysis_queue import LLMAnalysisQueue
from app.storage.postgres.client import pg_client

#: Durable queue for LLM work. A module-level instance keeps it importable by
#: tests; the pool is attached in main() once pg_client has connected.
analysis_queue = LLMAnalysisQueue(None)
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
#: Resolved once so the Batch API path can check the provider without
#: re-deriving it (and without instantiating a client just to ask).
LLM_PROVIDER_NAME = (
    "groq" if "groq.com" in HOSTED_LLM_URL.lower()
    else "google" if "googleapis.com" in HOSTED_LLM_URL.lower()
    else "generic"
) if LLM_PROVIDER == "hosted" else "ollama"
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

# --- Durable queue + batching configuration ---
# Items are enqueued on consume and drained by run_analysis_batches(). These
# knobs decide how much work is in flight at once and how often the queue is
# polled, which is what actually bounds the rate the LLM provider is hit at.
QUEUE_ENABLED = os.getenv("LLM_QUEUE_ENABLED", "true").strip().lower() in ("1", "true", "yes")
# Items claimed per batch. Larger batches amortise the prompt preamble and the
# RAG round-trip across more documents; too large and a single rate-limit
# response stalls the whole batch.
LLM_BATCH_SIZE = max(1, int(os.getenv("LLM_BATCH_SIZE", str(MAX_CONCURRENT_TASKS))))
# How long to wait when the queue came back empty. Long enough not to spin, short
# enough that an idle pipeline reacts to a new job promptly.
LLM_BATCH_POLL_SECONDS = float(os.getenv("LLM_BATCH_POLL_SECONDS", "2"))
# A claimed item whose worker dies is reclaimed after this long. Must exceed the
# worst-case single analysis (LLM timeout + retries + RAG), or healthy slow work
# gets handed to a second worker while the first is still running.
LLM_QUEUE_LEASE_SECONDS = float(os.getenv("LLM_QUEUE_LEASE_SECONDS", "600"))
LLM_QUEUE_MAX_ATTEMPTS = max(1, int(os.getenv("LLM_QUEUE_MAX_ATTEMPTS", "5")))
LLM_QUEUE_RETRY_BASE_SECONDS = float(os.getenv("LLM_QUEUE_RETRY_BASE_SECONDS", "30"))
WORKER_INSTANCE_ID = os.getenv("WORKER_INSTANCE_ID", f"{WORKER_TYPE}-{os.getpid()}")

# --- Groq Batch API (half-price bulk analysis) ---
# Two paths share one queue. The synchronous batch loop above is the default:
# results land in seconds. When the backlog is large enough that latency no
# longer matters — a bulk backfill, a wide discovery fan-out — items can instead
# be handed to Groq's Batch API for 50% of the price and no rate-limit impact,
# at the cost of a 24h-7d completion window.
BATCH_API_ENABLED = os.getenv("GROQ_BATCH_ENABLED", "false").strip().lower() in (
    "1", "true", "yes",
)
# Only hand work to the Batch API once the backlog is this deep. Below it, the
# synchronous path is both faster and no more expensive in aggregate.
BATCH_BACKLOG_THRESHOLD = max(1, int(os.getenv("GROQ_BATCH_MIN_BACKLOG", "200")))
# How many requests go into one batch file. Groq allows 50,000 lines; a few
# thousand keeps a single rejected payload from invalidating days of work.
BATCH_MAX_REQUESTS = max(1, int(os.getenv("GROQ_BATCH_MAX_REQUESTS", "2000")))
BATCH_COMPLETION_WINDOW = os.getenv("GROQ_BATCH_WINDOW", "24h")
BATCH_POLL_SECONDS = float(os.getenv("GROQ_BATCH_POLL_SECONDS", "900"))

#: Outcomes of analysing one queued item.
ANALYSIS_DONE = "done"        # analysed, or already analysed — drop from the queue
ANALYSIS_RETRY = "retry"      # transient — put back with backoff
ANALYSIS_FAILED = "failed"    # permanent — park it, stop retrying

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
            "analysis_source": "fallback",
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
            "analysis_source": "unavailable",
        }


# Groq SDK bounds. The SDK defaults (600s timeout, silent retries) are sized for
# healthy networks; when the network stalls these turn a single chat completion
# into a multi-minute hang. See extract_intelligence: the call also runs in a
# worker thread so the event loop (health server + Kafka consumer) never freezes.
GROQ_TIMEOUT_SECONDS = float(os.getenv("GROQ_TIMEOUT_SECONDS", "60"))
GROQ_MAX_RETRIES = int(os.getenv("GROQ_MAX_RETRIES", "2"))
# Token budget per completion. gpt-oss models are *reasoning* models: they emit a
# `reasoning` block that is billed as completion tokens. At 1024 tokens a real
# article prompt spent the entire budget reasoning and returned
# finish_reason="length" with content="" — which this code used to treat as
# "the model answered but we could not read it", i.e. a heuristic fallback. The
# whole corpus was being classified without the model while looking successful.
GROQ_MAX_COMPLETION_TOKENS = int(os.getenv("GROQ_MAX_COMPLETION_TOKENS", "4096"))
GROQ_REASONING_EFFORT = os.getenv("GROQ_REASONING_EFFORT", "low")
# Escalated budget used on the retry that follows a truncated answer.
GROQ_TRUNCATED_RETRY_TOKENS = int(os.getenv("GROQ_TRUNCATED_RETRY_TOKENS", "8192"))


class LLMResponseTruncated(RuntimeError):
    """The model ran out of tokens before it produced an answer.

    Transient by nature: the same request with a larger budget usually
    succeeds, so the queue retries it rather than accepting the heuristic
    fallback as a final answer.
    """


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
        # Raised after a truncated answer so the retry gets a bigger budget.
        self._max_completion_tokens = GROQ_MAX_COMPLETION_TOKENS
        self.groq_client = None
        if self.provider == "groq" and api_key and Groq is not None:
            self.groq_client = Groq(
                api_key=api_key,
                timeout=GROQ_TIMEOUT_SECONDS,
                max_retries=GROQ_MAX_RETRIES,
            )

    def _raise_if_truncated(self, data: dict) -> None:
        """Turn a token-truncated answer into a retryable error.

        Without this, a reasoning model that spent its whole budget thinking
        returns ``content=""`` with ``finish_reason="length"``. That used to
        fall through to the "model answered, JSON unreadable" branch and be
        recorded as a heuristic classification — indistinguishable, downstream,
        from a real one.
        """
        choices = (data or {}).get("choices") or []
        if not choices:
            return
        choice = choices[0] or {}
        finish_reason = choice.get("finish_reason")
        message = choice.get("message") or {}
        content = (message.get("content") or "").strip()
        if finish_reason != "length" and content:
            return
        if finish_reason == "length" and not content:
            bigger = GROQ_TRUNCATED_RETRY_TOKENS
            self._max_completion_tokens = max(self._max_completion_tokens, bigger)
            raise LLMResponseTruncated(
                f"model hit the {self._max_completion_tokens}-token completion limit "
                f"without producing an answer (reasoning model); retrying with a "
                f"larger budget"
            )
        if not content:
            reasoning = (message.get("reasoning") or "").strip()
            raise LLMResponseTruncated(
                "model returned an empty answer"
                + (f" (only {len(reasoning)} chars of reasoning)" if reasoning else "")
            )

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
        if not parsed_text or len(parsed_text.strip()) < MIN_ANALYSIS_CHARS:
            return {
                "source_type": DEFAULT_SOURCE_TYPE,
                "category": DEFAULT_INTELLIGENCE_CATEGORY,
                "threat_severity": 1,
                "entities": [],
                "summary": "Unable to analyze reliably; fallback inference used.",
                "llm_score": 0.0,
                "analysis_source": "unavailable",
            }

        if not self.api_key and FALLBACK_ONLY:
            logger.warning("No hosted API key available; using fallback analyzer in fallback-only mode.")
            return OllamaLLMClient._fallback_analyze(parsed_text=parsed_text, url=url)

        prompt = build_analysis_prompt(parsed_text, url, rag_context, category_hints)

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
                # The groq SDK is synchronous: calling it directly would block the
                # event loop for the whole timeout (health server, metrics and the
                # Kafka consumer all freeze with it). to_thread keeps the loop live.
                completion = await asyncio.to_thread(
                    self.groq_client.chat.completions.create,
                    model=self.model or "openai/gpt-oss-120b",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_completion_tokens=self._max_completion_tokens,
                    top_p=1,
                    reasoning_effort=GROQ_REASONING_EFFORT,
                    stream=False,
                    stop=None,
                )
                data = completion.model_dump() if hasattr(completion, "model_dump") else completion
                self._raise_if_truncated(data)
                response_text = self._extract_text_from_payload(data)
                # A completed answer resets the escalated budget, so one hard
                # document does not permanently double everyone's token spend.
                self._max_completion_tokens = GROQ_MAX_COMPLETION_TOKENS
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
                intelligence["analysis_source"] = "llm"
            except Exception:
                intelligence = {
                    "source_type": DEFAULT_SOURCE_TYPE,
                    "category": DEFAULT_INTELLIGENCE_CATEGORY,
                    "threat_severity": 1,
                    "entities": [],
                    "summary": response_text if response_text else "Unable to analyze reliably; fallback inference used.",
                    "llm_score": 0.0,
                    # A model answered but its JSON could not be read, so the
                    # labels below are defaults rather than the model's own.
                    "analysis_source": "partial",
                }

            return intelligence
        except Exception as e:
            # A rejected key is permanent: retrying it for every item burns the
            # budget and hides an operator-actionable fault behind a wall of
            # identical warnings. Name it explicitly and rate-limit the log so
            # the cause stays visible instead of scrolling past as noise.
            reason = _classify_llm_error(e)
            llm_request_errors.labels(reason=reason).inc()

            # Transient faults must NOT become a stored heuristic label. A daily
            # quota (429 "try again in 12m") or a truncated completion will both
            # clear on their own, and falling back here marks the item analysed
            # forever - the model never gets to see it again, and a whole quota
            # window's worth of corpus silently becomes rule-based output.
            # Let the error propagate so the queue retries with backoff.
            if isinstance(e, LLMResponseTruncated) or _looks_transient(e):
                raise

            if reason in _PERMANENT_LLM_ERRORS:
                global _last_permanent_llm_error_at
                now = time.monotonic()
                if now - _last_permanent_llm_error_at > _PERMANENT_LLM_ERROR_LOG_INTERVAL:
                    _last_permanent_llm_error_at = now
                    logger.error(
                        "Hosted LLM is PERMANENTLY UNAVAILABLE (%s): %s. Every item is "
                        "being classified by the rule-based fallback instead of the model. "
                        "Fix HOSTED_LLM_API_KEY (or the account) - classifications and "
                        "extracted entities are NOT real model output until then.",
                        reason, e,
                    )
            logger.warning("Hosted LLM request failed (%s): %s", reason, e)
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


MAX_ANALYSIS_CHARS = 2500
#: Below this, an item is not worth a model call at all — the response is
#: dominated by "unable to analyze".
MIN_ANALYSIS_CHARS = 50

SOURCE_TYPE_VALUES = (
    "news, forum, blog, social, government, academic, ecommerce, other"
)


def build_analysis_prompt(
    parsed_text: str,
    url: str,
    rag_context: str = "",
    category_hints: str = "",
) -> str:
    """The single analysis prompt, shared by the synchronous and batch paths.

    Extracted deliberately: the Groq Batch API submits *pre-rendered* requests,
    so if the batch path assembled its own prompt the two paths would drift and
    a batched item would be classified under different rules than a synchronous
    one — with no way to tell afterwards, because the prompt is not stored.
    """
    category_list_str = "\n".join(
        f'  - "{cat}": {desc}' for cat, desc in CATEGORY_DESCRIPTIONS.items()
    )
    content_language = HostedLLMClient._detect_content_language(parsed_text)
    summary_language = "Amharic" if content_language == "am" else "English"
    rag_block = f"\n{rag_context.strip()}\n" if rag_context else ""
    hints_block = f"\n{category_hints.strip()}\n" if category_hints else ""
    if len(parsed_text) <= MAX_ANALYSIS_CHARS:
        full_content = parsed_text
    else:
        half = MAX_ANALYSIS_CHARS // 2
        full_content = (
            f"{parsed_text[:half]}\n\n[...middle omitted for provider limit...]\n\n"
            f"{parsed_text[-half:]}"
        )
    return f"""You are a careful content analyst. Read the full content below, not just the headline or first paragraph.

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

Also classify the source type as one of: {SOURCE_TYPE_VALUES}

Return ONLY valid JSON with this exact structure:
{{
  "source_type": one of [{SOURCE_TYPE_VALUES}],
  "category": one of {sorted(ALL_INTELLIGENCE_CATEGORIES)},
  "threat_severity": integer from 1-5 (1=low, 5=critical),
  "entities": list of extracted entities (names, emails, IPs, domains, sensitive info, locations),
  "summary": a concise 2-4 sentence summary in {summary_language} of the main idea, most important facts, and why it matters,
  "confidence": a float between 0 and 1 reflecting your certainty in the category classification
}}"""


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
        # Same site-folder layout as the raw/parsed objects, so one item's raw
        # HTML, parsed JSON and audit snapshot sit side by side in the browser.
        object_name = f"{site_from_url(url)}/{item_id}_llm_context.json"
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
                        intelligence.analysis_source,
                        intelligence.llm_model,
                        intelligence.llm_score,
                        intelligence.created_at,
                    ]],
                    column_names=[
                        "job_id", "item_id", "url", "source_type", "category", "threat_severity",
                        "entities", "summary", "language", "analysis_source",
                        "llm_model", "llm_score", "created_at",
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
        parsed_item = _parse_item_message(message_value)
        if parsed_item is None:
            return
        await analyze_parsed_item(parsed_item)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing parsed item: {e}", exc_info=True)


def _parse_item_message(message_value: bytes) -> "ParsedItem | None":
    """Decode a crawl.parsed message. None when it must not be analysed."""
    data = json.loads(message_value)
    parsed_item = ParsedItem(**data)

    logger.info(
        f"Processing parsed item: job={parsed_item.job_id} item={parsed_item.item_id} url={parsed_item.url}"
    )

    # Skip items the pipeline already flagged as failures — they are not
    # worth an LLM call (language-rejected content is handled upstream too).
    if parsed_item.status == "failed":
        logger.info(f"Skipping failed item {parsed_item.item_id}")
        return None
    return parsed_item


async def enqueue_parsed_item(message_value: bytes) -> None:
    """Consume-time work: decode the message and put it on the durable queue.

    Deliberately does nothing expensive. The Kafka offset is committed as soon
    as this returns, so the queue — not the consumer's in-memory state — is what
    survives a restart. Everything slow (RAG retrieval, the LLM call, the
    ClickHouse write) happens in :func:`analyze_parsed_item`, driven by
    :func:`run_analysis_batches`.
    """
    try:
        parsed_item = _parse_item_message(message_value)
        if parsed_item is None:
            return

        # Idempotency pre-check: re-delivery after a crash must not re-analyze
        # (and must not duplicate ClickHouse rows).
        if await is_intelligence_processed(parsed_item.item_id):
            logger.info(f"Item {parsed_item.item_id} already intelligence_processed — skipping")
            return

        queued = await analysis_queue.enqueue(
            item_id=parsed_item.item_id,
            job_id=parsed_item.job_id,
            payload=json.loads(message_value),
        )
        if queued:
            logger.debug(f"Queued {parsed_item.item_id} for LLM analysis")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON message: {e}")
    except Exception as e:
        logger.error(f"Failed to queue parsed item: {e}", exc_info=True)


async def analyze_parsed_item(parsed_item) -> str:
    """Run the LLM analysis for one item and persist everything it produces.

    Returns one of the three ``ANALYSIS_*`` outcomes. The distinction matters:
    the old inline path swallowed every failure, so a rate-limited provider
    looked identical to a successful run until you noticed nothing was being
    written anywhere.
    """
    try:
        # Idempotency re-check: a redelivered queue row, or a retry after a
        # partial failure, must not produce a second ClickHouse row.
        if await is_intelligence_processed(parsed_item.item_id):
            logger.info(f"Item {parsed_item.item_id} already intelligence_processed — skipping")
            return ANALYSIS_DONE

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
        except Exception as exc:
            # Not settled: the queue retries with backoff, and the item keeps
            # intelligence_processed=false until an analysis really lands.
            return ANALYSIS_RETRY if _looks_transient(exc) else ANALYSIS_FAILED

        await _persist_analysis(
            parsed_item,
            intelligence_data,
            rag_context=rag_context,
            category_hints=category_hints,
            rag_embedding=rag_embedding,
            extracted_text=extracted_text,
        )
        return ANALYSIS_DONE

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON message: {e}")
        return ANALYSIS_FAILED
    except Exception as e:
        logger.error(f"Unexpected error analysing parsed item: {e}", exc_info=True)
        return ANALYSIS_RETRY


async def _maybe_raise_alert(parsed_item, intelligence) -> None:
    """Mirror a high-severity finding into the alert feed and email the owner.

    Never raises: alerting is downstream of the analysis, and a mail server
    outage must not turn a successfully-analysed item into a retried one. The
    worst case of swallowing the error here is a missed notification, which is
    strictly better than an infinite re-analysis loop burning LLM quota.
    """
    try:
        alert_id = await record_alert(
            job_id=parsed_item.job_id,
            item_id=parsed_item.item_id,
            url=parsed_item.url,
            title=getattr(parsed_item, "title", "") or "",
            category=intelligence.category,
            severity=intelligence.threat_severity,
            language=parsed_item.language or "",
            summary=intelligence.summary,
            entities=list(intelligence.entities or []),
            analysis_source=intelligence.analysis_source,
            llm_model=intelligence.llm_model,
        )
    except Exception:
        logger.exception("Could not raise alert for item %s", parsed_item.item_id)
        return
    if not alert_id:
        return
    try:
        await notify_alert_email(parsed_item.job_id, alert_id)
    except Exception:
        logger.exception("Alert email delivery failed for %s", alert_id)


async def _persist_analysis(
    parsed_item,
    intelligence_data: dict,
    *,
    rag_context: str = "",
    category_hints: str = "",
    rag_embedding=None,
    extracted_text: str = "",
) -> None:
    """Write one analysis result everywhere it is needed.

    Shared by the synchronous path and the Groq Batch path so a batched item is
    stored identically — same ClickHouse rows, same Postgres flag, same Qdrant
    embedding. Anything that differed here would make batched results silently
    invisible to parts of the product.
    """
    if True:
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
            analysis_source=str(intelligence_data.get("analysis_source") or "llm"),
            llm_model=llm_model,
            llm_score=confidence,
        )

        # A row the heuristic produced must never be counted as model output.
        # Both the reporting endpoints and the human-evaluation accuracy metric
        # read this column, so a dead LLM shows up as "0 analyzed" instead of
        # silently scoring as a healthy classifier.
        if intelligence.analysis_source != "llm":
            logger.warning(
                "[%s] Item %s analyzed WITHOUT an LLM (source=%s, model=%s) - "
                "classification is heuristic, not model output",
                parsed_item.job_id, parsed_item.item_id,
                intelligence.analysis_source, llm_model,
            )
            # The label is mandatory on a labelled Counter. Calling inc() bare
            # raised ValueError here, which aborted the item *before* the
            # ClickHouse write below — so every fallback analysis was silently
            # discarded and retried until its attempts ran out.
            llm_fallback_items.labels(source=intelligence.analysis_source).inc()

        # Typed entities for the intelligence_entities table: emails, IPs,
        # domains, everything else. Best-effort — regex only, never fails.
        entities_by_type = _typed_entities(intelligence.entities)

        # Ingest to ClickHouse (analytics + flattened entity rows)
        await ingest_intelligence_to_clickhouse(intelligence, entities_by_type)

        # High-severity alerting. Inside _persist_analysis so both the sync and
        # the Groq Batch path raise alerts identically - a batched item that
        # scored 5 must not be the one item nobody is paged for.
        await _maybe_raise_alert(parsed_item, intelligence)

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


def _parse_batch_analysis(content: str) -> dict:
    """Turn a batched model's raw text into the intelligence dict.

    Reuses the same tolerant parsing as the synchronous path: models wrap JSON
    in prose or fences often enough that a strict ``json.loads`` would throw
    away a perfectly good, already-paid-for result.
    """
    parsed = OllamaLLMClient._parse_intelligence_response(content)
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError("batch result did not contain a JSON object")
    return parsed


def _item_text_and_url(payload: dict) -> tuple[str, str]:
    data = payload.get("data")
    if isinstance(data, dict):
        return data.get("extracted_text") or "", payload.get("url") or ""
    return "", payload.get("url") or ""


def _batch_client() -> "GroqBatchClient | None":
    """A Groq Batch client, or None when the provider isn't Groq/unconfigured."""
    if LLM_PROVIDER != "hosted" or LLM_PROVIDER_NAME != "groq":
        return None
    key = os.getenv("HOSTED_LLM_API_KEY", "")
    if not key:
        logger.info("Groq Batch API unavailable: no HOSTED_LLM_API_KEY")
        return None
    return GroqBatchClient(
        key,
        completion_window=BATCH_COMPLETION_WINDOW,
    )


async def submit_pending_batch() -> str | None:
    """Move a deep backlog into Groq's Batch API. Returns the batch id.

    Only runs when the backlog is past ``BATCH_BACKLOG_THRESHOLD``: below that
    depth the synchronous path is both faster and no dearer overall, and handing
    a handful of items to a job with a 24-hour window would be actively wrong.
    """
    client = _batch_client()
    if client is None:
        return None

    depth = await analysis_queue.depth()
    pending = depth.get("pending", 0)
    if pending < BATCH_BACKLOG_THRESHOLD:
        logger.debug(
            "Batch API idle: backlog %d < threshold %d", pending, BATCH_BACKLOG_THRESHOLD
        )
        return None

    claimed = await analysis_queue.claim(BATCH_MAX_REQUESTS, worker_id=WORKER_INSTANCE_ID)
    if not claimed:
        return None

    requests: list[tuple[str, str, str]] = []
    batchable: list[tuple[str, str]] = []
    skipped: list[str] = []
    for item in claimed:
        text, url = _item_text_and_url(item.payload)
        if not text or len(text.strip()) < MIN_ANALYSIS_CHARS:
            skipped.append(item.item_id)
            continue
        requests.append((item.item_id, "", build_analysis_prompt(text, url)))
        batchable.append((item.item_id, WORKER_INSTANCE_ID))

    # Short and empty items never reach the model: settle them synchronously
    # rather than paying Groq to tell us what we already know.
    await analysis_queue.settle_success(skipped)

    if not requests:
        return None

    from app.services.groq_batch import build_batch_lines

    try:
        submission = await client.submit(
            build_batch_lines(requests), model=HOSTED_LLM_MODEL
        )
    except GroqBatchError as exc:
        # Put everything back: the batch never went out, so nothing was paid for.
        for item_id, _ in batchable:
            await analysis_queue.fail(
                item_id, f"batch submission failed: {exc}", retry_in_seconds=30
            )
        logger.error("Groq batch submission failed; %d item(s) returned to the queue",
                     len(batchable))
        return None

    moved = await analysis_queue.mark_batched(batchable, batch_id=submission.batch_id)
    logger.info(
        "Handed %d item(s) to Groq batch %s (%d settled locally as too short)",
        moved, submission.batch_id, len(skipped),
    )
    if moved < len(batchable):
        logger.warning(
            "Only %d of %d claimed items moved to batched state; the rest stay "
            "claimable and will be analysed synchronously",
            moved, len(batchable),
        )
    return submission.batch_id


async def collect_batch_results(batch_id: str) -> int:
    """Poll one batch and write whatever has come back. Returns items settled."""
    client = _batch_client()
    if client is None:
        return 0

    results = await client.collect(batch_id)
    if not results:
        return 0

    settled: list[str] = []
    failed: list[tuple[str, str]] = []
    for custom_id, (content, error) in results.items():
        if error or not content:
            failed.append((custom_id, error or "empty result"))
            continue
        rows = await analysis_queue.rows_for_batch([custom_id])
        for row in rows:
            try:
                parsed_item = ParsedItem(**row["payload"])
                intelligence_data = _parse_batch_analysis(content)
                # Route through the same persistence path as a synchronous item so
                # a batched result is indistinguishable downstream.
                await _persist_analysis(parsed_item, intelligence_data)
                settled.append(custom_id)
            except Exception as exc:
                logger.error(
                    "Could not persist batched result for %s: %s", custom_id, exc, exc_info=True
                )
                failed.append((custom_id, str(exc)))

    await analysis_queue.settle_success(settled)
    for item_id, reason in failed:
        await analysis_queue.fail(item_id, f"batch result unusable: {reason}",
                                  retry_in_seconds=None)
    logger.info(
        "Batch %s: %d result(s) written, %d unusable", batch_id, len(settled), len(failed)
    )
    return len(settled)


async def process_message_safely(message_value: bytes):
    async with llm_semaphore:
        await process_parsed_item(message_value)


async def _process_queued_item(item) -> str:
    """Analyse one claimed queue row and settle it. Never raises."""
    try:
        parsed_item = ParsedItem(**item.payload)
    except Exception as exc:
        # A payload that no longer matches the schema will never parse; retrying
        # it forever would block the queue head for no gain.
        logger.error("Queue row %s has an unparseable payload: %s", item.item_id, exc)
        return ANALYSIS_FAILED

    try:
        return await analyze_parsed_item(parsed_item)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error("Queued analysis crashed for %s: %s", item.item_id, exc, exc_info=True)
        return ANALYSIS_RETRY


async def _settle_queued_item(item) -> tuple[str, str]:
    """Analyse one claimed row and record the outcome. Returns (item_id, outcome)."""
    async with llm_semaphore:
        outcome = await _process_queued_item(item)

    if outcome == ANALYSIS_RETRY and item.attempts >= LLM_QUEUE_MAX_ATTEMPTS:
        logger.error(
            "Giving up on %s after %d attempt(s); it stays unanalysed and "
            "visible in the queue rather than looping forever",
            item.item_id, item.attempts,
        )
        await analysis_queue.fail(
            item.item_id, f"exceeded {LLM_QUEUE_MAX_ATTEMPTS} attempts", retry_in_seconds=None
        )
        return item.item_id, ANALYSIS_FAILED

    if outcome == ANALYSIS_RETRY:
        # Exponential backoff per item: a provider-wide 429 should not see every
        # item in the batch retry in lockstep.
        delay = LLM_QUEUE_RETRY_BASE_SECONDS * (2 ** (item.attempts - 1))
        await analysis_queue.fail(
            item.item_id, "transient analysis failure", retry_in_seconds=delay
        )
    elif outcome == ANALYSIS_FAILED:
        await analysis_queue.fail(
            item.item_id, "permanent analysis failure", retry_in_seconds=None
        )
    return item.item_id, outcome


async def run_analysis_batches() -> None:
    """Drain the durable queue in bounded batches until cancelled.

    Claims up to ``LLM_BATCH_SIZE`` items and analyses the whole batch
    concurrently (each task still holds ``llm_semaphore``, so the number of
    simultaneous provider calls stays at ``MAX_CONCURRENT_TASKS`` no matter how
    many batches are claimed). Returns nothing; runs forever.
    """
    loop = asyncio.get_running_loop()
    last_reclaim = 0.0

    while True:
        try:
            now = loop.time()
            if now - last_reclaim >= LLM_QUEUE_LEASE_SECONDS:
                last_reclaim = now
                reclaimed = await analysis_queue.reclaim_stalled(
                    lease_seconds=LLM_QUEUE_LEASE_SECONDS
                )
                if reclaimed:
                    logger.warning(
                        "Reclaimed %d LLM queue item(s) whose worker lease expired",
                        reclaimed,
                    )

            claimed = await analysis_queue.claim(
                LLM_BATCH_SIZE, worker_id=WORKER_INSTANCE_ID
            )
            if not claimed:
                await asyncio.sleep(LLM_BATCH_POLL_SECONDS)
                continue

            logger.info(
                "Analysing %d queued item(s): %s",
                len(claimed), ", ".join(item.item_id for item in claimed[:8]),
            )
            results = await asyncio.gather(
                *(_settle_queued_item(item) for item in claimed),
                return_exceptions=True,
            )
            settled = [
                item_id
                for item_id, outcome in (
                    (r[0], r[1]) if isinstance(r, tuple) else ("", "") for r in results
                )
                if outcome == ANALYSIS_DONE
            ]
            await analysis_queue.settle_success(settled)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The loop must outlive any single failure, or one bad batch stops
            # LLM analysis for the lifetime of the process.
            logger.error("LLM batch loop error (continuing): %s: %s", type(exc).__name__, exc)
            await asyncio.sleep(LLM_BATCH_POLL_SECONDS)


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

    # The queue needs a pool of its own view of the same database; reuse the one
    # pg_client already opened rather than doubling the connection count.
    analysis_queue._pool = pg_client.system_pool
    await analysis_queue.ensure_schema()

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

    batch_task: asyncio.Task | None = None
    try:
        # Consume = decode + enqueue, nothing more. The offset is committed as
        # soon as the row exists, so the durable queue owns the work from that
        # moment on: a crash here loses nothing that was not already persisted.
        if QUEUE_ENABLED:
            batch_task = asyncio.create_task(run_analysis_batches())
            logger.info(
                "LLM analysis running from the durable queue (batch=%d, "
                "max_concurrent=%d, poll=%.1fs)",
                LLM_BATCH_SIZE, MAX_CONCURRENT_TASKS, LLM_BATCH_POLL_SECONDS,
            )

        async for msg in consumer:
            metrics.record_consumed()
            try:
                if QUEUE_ENABLED:
                    await enqueue_parsed_item(msg.value)
                    metrics.record_processed("success")
                else:
                    # Escape hatch: the original inline behaviour, for when the
                    # database is unavailable and only Kafka is durable.
                    async with llm_semaphore:
                        await process_parsed_item(msg.value)
                    metrics.record_processed("success")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Forward progress is preserved and unprocessable items are not
                # redelivered forever.
                metrics.record_error("processing_error")
                metrics.record_processed("error")
                logger.error("Message processing failed: %s", exc, exc_info=True)
            # Commit regardless of processing outcome.
            await _commit(consumer)
    except asyncio.CancelledError:
        logger.info("LLM worker cancellation requested.")
    finally:
        logger.info("Shutting down LLM worker gracefully...")
        if batch_task is not None:
            batch_task.cancel()
            try:
                await batch_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.debug("LLM batch loop ended with %s", exc)
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
