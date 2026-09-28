"""Unit tests for the llm-worker RAG wiring.

Covers:
- rag_get_context: embedding + search similar → formatted context block
- rag_store_embedding: upsert into Qdrant with intelligence metadata
- process_parsed_item: RAG context injected into the LLM prompt, article
  embedded + upserted when RAG_ENABLED, and skipped entirely when disabled
- Prompt injection: both Ollama and hosted LLM clients receive the RAG
  context block inside the prompt

No real services (Ollama / Qdrant / ClickHouse / Kafka) are contacted.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

# ═════════════════════════════════════════════════════════════════════
# Pre-import mocks (same pattern as the deep-worker unit test)
# ═════════════════════════════════════════════════════════════════════

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "../..")
sys.path.insert(0, _PROJECT_ROOT)

# Prompt-audit snapshots write to MinIO — a real network service. Keep this
# module's contract of contacting no real services by disabling snapshots
# for the imported worker module (restored after import below).
os.environ["PROMPT_AUDIT_ENABLED"] = "false"

_MOCK_MODULES = [
    "clickhouse_connect",
    "aiokafka", "aiokafka.consumer", "aiokafka.consumer.group_coordinator",
    "aiokafka.consumer.subscription_state", "aiokafka.producer", "aiokafka.producer.producer",
    "app.storage.postgres.client",
    "workers.common",
]

for mod_name in _MOCK_MODULES:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# Import the module
_spec = importlib.util.spec_from_file_location(
    "workers.llm_worker_main",
    os.path.join(_PROJECT_ROOT, "workers", "llm-worker", "main.py"),
    submodule_search_locations=[],
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["workers.llm_worker_main"] = _mod
_spec.loader.exec_module(_mod)

# Restore any sys.modules entries we shadowed with MagicMocks so subsequently
# collected test files are not poisoned.
for mod_name in _MOCK_MODULES:
    mod = sys.modules.get(mod_name)
    if isinstance(mod, MagicMock):
        sys.modules.pop(mod_name, None)
sys.modules.pop("workers.llm_worker_main", None)
os.environ.pop("PROMPT_AUDIT_ENABLED", None)

rag_get_context = _mod.rag_get_context
rag_store_embedding = _mod.rag_store_embedding
process_parsed_item = _mod.process_parsed_item
RAG_ENABLED = _mod.RAG_ENABLED
OllamaLLMClient = _mod.OllamaLLMClient
HostedLLMClient = _mod.HostedLLMClient
ParsedItem = _mod.ParsedItem


# ═════════════════════════════════════════════════════════════════════
# Fakes
# ═════════════════════════════════════════════════════════════════════

class FakeEmbeddingClient:
    def __init__(self):
        self.is_available = AsyncMock(return_value=True)
        self.embed = AsyncMock(return_value=[0.1, 0.2, 0.3])
        self.pull_model = AsyncMock(return_value=True)


class FakeVectorDBClient:
    def __init__(self, similar=None):
        self.is_available = AsyncMock(return_value=True)
        self.create_collection = AsyncMock(return_value=True)
        self.store_embedding = AsyncMock(return_value=True)
        self.search_similar = AsyncMock(return_value=similar or [])


def make_article(text: str | None = None) -> bytes:
    """Build a crawl.parsed Kafka message for one article."""
    text = text or "A long enough sample article body. " * 40
    item = ParsedItem(
        job_id="JOB00000001",
        item_id="ITEM00000001",
        url="https://example.com/article",
        worker="surface",
        language="en",
        data={
            "extracted_text": text,
            "character_count": len(text),
            "original_status_code": 200,
        },
    )
    return json.dumps(item.model_dump()).encode()


@pytest.fixture
def rag_clients(monkeypatch):
    """Install fake embedding + vector DB clients and enable RAG."""
    embedding = FakeEmbeddingClient()
    vector_db = FakeVectorDBClient(
        similar=[{
            "item_id": "ITEM00000002",
            "url": "https://example.com/similar",
            "language": "en",
            "category": "gov_issue",
            "text_summary": "Another related article summary",
            "score": 0.87,
        }]
    )
    monkeypatch.setattr(_mod, "embedding_client", embedding)
    monkeypatch.setattr(_mod, "vector_db_client", vector_db)
    monkeypatch.setattr(_mod, "RAG_ENABLED", True)
    return embedding, vector_db


# ═════════════════════════════════════════════════════════════════════
# rag_get_context
# ═════════════════════════════════════════════════════════════════════

async def test_rag_get_context_returns_formatted_context(rag_clients):
    embedding, vector_db = rag_clients
    context, emb, anchor_hits = await rag_get_context(
        "Some sufficiently long parsed text. " * 20, "en", exclude_item_id="ITEM00000001"
    )

    assert emb == [0.1, 0.2, 0.3]
    assert "SIMILAR ARTICLES FOR REFERENCE" in context
    assert "https://example.com/similar" in context
    embedding.embed.assert_awaited_once()
    vector_db.search_similar.assert_awaited_once()
    kwargs = vector_db.search_similar.await_args.kwargs
    assert kwargs["top_k"] == _mod.RAG_TOP_K
    assert kwargs["language_filter"] == "en"
    # Self-exclusion: the analyzed item must never retrieve itself as context.
    assert kwargs["exclude_item_id"] == "ITEM00000001"
    assert isinstance(anchor_hits, list)


async def test_rag_get_context_skips_unknown_language_filter(rag_clients):
    _, vector_db = rag_clients
    await rag_get_context("Some sufficiently long parsed text. " * 20, "unknown")
    assert vector_db.search_similar.await_args.kwargs["language_filter"] is None


async def test_rag_get_context_no_similar_returns_empty_context(rag_clients):
    embedding, vector_db = rag_clients
    vector_db.search_similar.return_value = []
    context, emb, _ = await rag_get_context("Some sufficiently long parsed text. " * 20, "en")
    assert context == ""
    assert emb == [0.1, 0.2, 0.3]  # embedding still reusable for the upsert


async def test_rag_get_context_disabled_skips_embeddings(monkeypatch):
    embedding = FakeEmbeddingClient()
    vector_db = FakeVectorDBClient()
    monkeypatch.setattr(_mod, "embedding_client", embedding)
    monkeypatch.setattr(_mod, "vector_db_client", vector_db)
    monkeypatch.setattr(_mod, "RAG_ENABLED", False)

    context, emb, _ = await rag_get_context("Some sufficiently long parsed text. " * 20, "en")
    assert context == ""
    assert emb is None
    embedding.embed.assert_not_awaited()
    vector_db.search_similar.assert_not_awaited()


async def test_rag_get_context_degrades_when_embedding_down(rag_clients):
    embedding, vector_db = rag_clients
    embedding.is_available.return_value = False
    context, emb, _ = await rag_get_context("Some sufficiently long parsed text. " * 20, "en")
    assert context == ""
    assert emb is None
    embedding.embed.assert_not_awaited()


# ═════════════════════════════════════════════════════════════════════
# rag_store_embedding
# ═════════════════════════════════════════════════════════════════════

async def test_rag_store_embedding_upserts_with_metadata(rag_clients):
    embedding, vector_db = rag_clients
    ok = await rag_store_embedding(
        item_id="ITEM00000001",
        url="https://example.com/article",
        text="Some sufficiently long parsed text. " * 20,
        language="en",
        category="cyber_threat",
        text_summary="A summary of the article",
        embedding=[0.9, 0.8, 0.7],
    )

    assert ok is True
    embedding.embed.assert_not_awaited()  # reuse of pre-computed embedding
    # create_collection is invoked inside store_embedding (vector-size guard)
    vector_db.store_embedding.assert_awaited_once()
    kwargs = vector_db.store_embedding.await_args.kwargs
    assert kwargs["item_id"] == "ITEM00000001"
    assert kwargs["embedding"] == [0.9, 0.8, 0.7]
    assert kwargs["vector_size"] == _mod.EMBEDDING_VECTOR_SIZE
    assert kwargs["metadata"]["category"] == "cyber_threat"
    assert kwargs["metadata"]["text_summary"] == "A summary of the article"
    assert kwargs["metadata"]["url"] == "https://example.com/article"


async def test_rag_store_embedding_disabled_is_noop(monkeypatch):
    embedding = FakeEmbeddingClient()
    vector_db = FakeVectorDBClient()
    monkeypatch.setattr(_mod, "embedding_client", embedding)
    monkeypatch.setattr(_mod, "vector_db_client", vector_db)
    monkeypatch.setattr(_mod, "RAG_ENABLED", False)

    ok = await rag_store_embedding(
        item_id="ITEM00000001",
        url="https://example.com/article",
        text="Some sufficiently long parsed text. " * 20,
    )
    assert ok is False
    embedding.embed.assert_not_awaited()
    vector_db.store_embedding.assert_not_awaited()


# ═════════════════════════════════════════════════════════════════════
# process_parsed_item (end-to-end wiring)
# ═════════════════════════════════════════════════════════════════════

class FakeLLMClient:
    def __init__(self):
        self.calls = []
        self.extract_intelligence = AsyncMock(
            side_effect=lambda text, url, rag_context="", category_hints="": self.calls.append(
                (url, rag_context, category_hints)
            ) or {
                "source_type": "news",
                "category": "gov_issue",
                "threat_severity": 2,
                "entities": [],
                "summary": "Article summary",
                "llm_score": 0.9,
                "confidence": 0.9,
            }
        )


def install_pipeline_fakes(monkeypatch, rag_clients, llm=None):
    embedding, vector_db = rag_clients
    monkeypatch.setattr(_mod, "llm_client", llm or FakeLLMClient())
    monkeypatch.setattr(_mod, "ingest_intelligence_to_clickhouse", AsyncMock())
    pg = MagicMock()
    pg.mark_item_intelligence_processed = AsyncMock(return_value=None)
    monkeypatch.setattr(_mod, "pg_client", pg)
    return embedding, vector_db


async def test_process_parsed_item_injects_rag_context_and_upserts(monkeypatch, rag_clients):
    embedding, vector_db = rag_clients
    llm = FakeLLMClient()
    install_pipeline_fakes(monkeypatch, rag_clients, llm=llm)

    await process_parsed_item(make_article())

    # RAG context injected into the LLM prompt
    assert llm.calls, "LLM should have been called"
    url, rag_context, category_hints = llm.calls[0]
    assert url == "https://example.com/article"
    assert "SIMILAR ARTICLES FOR REFERENCE" in rag_context
    assert isinstance(category_hints, str)

    # Article embedded once and upserted with the LLM category/summary
    embedding.embed.assert_awaited_once()
    vector_db.store_embedding.assert_awaited_once()
    stored_kwargs = vector_db.store_embedding.await_args.kwargs
    assert stored_kwargs["item_id"] == "ITEM00000001"
    assert stored_kwargs["metadata"]["category"] == "gov_issue"
    assert stored_kwargs["metadata"]["text_summary"] == "Article summary"


async def test_process_parsed_item_rag_disabled_skips_rag(monkeypatch):
    embedding = FakeEmbeddingClient()
    vector_db = FakeVectorDBClient()
    monkeypatch.setattr(_mod, "embedding_client", embedding)
    monkeypatch.setattr(_mod, "vector_db_client", vector_db)
    monkeypatch.setattr(_mod, "RAG_ENABLED", False)
    llm = FakeLLMClient()
    install_pipeline_fakes(monkeypatch, (embedding, vector_db), llm=llm)

    await process_parsed_item(make_article())

    assert llm.calls and llm.calls[0][1] == ""  # no RAG context
    embedding.embed.assert_not_awaited()
    vector_db.search_similar.assert_not_awaited()
    vector_db.store_embedding.assert_not_awaited()


async def test_process_parsed_item_rag_failures_do_not_break_pipeline(monkeypatch, rag_clients):
    embedding, vector_db = rag_clients
    embedding.embed.side_effect = RuntimeError("ollama unreachable")
    llm = FakeLLMClient()
    install_pipeline_fakes(monkeypatch, rag_clients, llm=llm)

    await process_parsed_item(make_article())

    # Pipeline still completes: LLM called without RAG context
    assert llm.calls and llm.calls[0][1] == ""


# ═════════════════════════════════════════════════════════════════════
# Prompt injection (both LLM clients)
# ═════════════════════════════════════════════════════════════════════

def _ollama_response_json() -> dict:
    return {
        "response": json.dumps({
            "source_type": "news",
            "category": "gov_issue",
            "threat_severity": 2,
            "entities": [],
            "summary": "Article summary",
        })
    }


async def test_ollama_prompt_includes_rag_context():
    client = OllamaLLMClient("http://ollama:11434", "qwen2:8b")
    resp = MagicMock(status_code=200)
    resp.json.return_value = _ollama_response_json()
    client.client = MagicMock()
    client.client.post = AsyncMock(return_value=resp)

    text = "Some sufficiently long parsed text. " * 40
    await client.extract_intelligence(text, "https://example.com/article",
                                      rag_context="SIMILAR ARTICLES FOR REFERENCE:\n...")

    prompt = client.client.post.await_args.kwargs["json"]["prompt"]
    assert "SIMILAR ARTICLES FOR REFERENCE" in prompt
    assert "https://example.com/article" in prompt


async def test_hosted_prompt_includes_rag_context():
    client = HostedLLMClient("http://llm.test/v1/chat/completions", "test-key", "some-model")
    resp = MagicMock(status_code=200)
    resp.json.return_value = _ollama_response_json()
    resp.text = json.dumps(_ollama_response_json())
    client.client = MagicMock()
    client.client.post = AsyncMock(return_value=resp)

    text = "Some sufficiently long parsed text. " * 40
    await client.extract_intelligence(text, "https://example.com/article",
                                      rag_context="SIMILAR ARTICLES FOR REFERENCE:\n...")

    prompt = client.client.post.await_args.kwargs["json"]["prompt"]
    assert "SIMILAR ARTICLES FOR REFERENCE" in prompt
    assert "https://example.com/article" in prompt