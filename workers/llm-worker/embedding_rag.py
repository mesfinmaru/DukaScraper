"""
RAG Embedding & Vector DB Module for llm-worker

Handles:
1. Embedding texts using Ollama (local, free) — multilingual model (bge-m3),
   chunked for long articles with mean-pooled article vectors
2. Storing embeddings in Qdrant Vector DB
3. Retrieving similar articles for context (with self-exclusion and a
   similarity score threshold so weak matches never reach the prompt)
"""

import hashlib
import logging
import uuid
from typing import List, Optional

import httpx

logger = logging.getLogger(__name__)

# Chunking configuration for long-article embeddings. bge-m3 supports up to
# 8192 tokens, but moderate chunks keep per-call latency low and let retrieval
# match specific sections rather than a whole-page average.
CHUNK_CHARS = 2000
CHUNK_OVERLAP = 200
MAX_CHUNKS_PER_ARTICLE = 8


def _chunk_text(text: str) -> List[str]:
    """Split text into overlapping chunks (deterministic, size-bounded)."""
    clean = (text or "").strip()
    if not clean:
        return []
    if len(clean) <= CHUNK_CHARS:
        return [clean]
    chunks: List[str] = []
    start = 0
    while start < len(clean) and len(chunks) < MAX_CHUNKS_PER_ARTICLE:
        end = min(start + CHUNK_CHARS, len(clean))
        chunks.append(clean[start:end])
        if end >= len(clean):
            break
        start = end - CHUNK_OVERLAP
    return chunks


def _mean_pool(vectors: List[List[float]]) -> Optional[List[float]]:
    """Mean-pool chunk embeddings into one article vector (L2-normalized)."""
    if not vectors:
        return None
    if len(vectors) == 1:
        return vectors[0]
    dim = len(vectors[0])
    acc = [0.0] * dim
    for vec in vectors:
        if len(vec) != dim:
            return None  # inconsistent dimensionality; give up on pooling
    for vec in vectors:
        for i, val in enumerate(vec):
            acc[i] += val
    n = float(len(vectors))
    pooled = [v / n for v in acc]
    norm = sum(v * v for v in pooled) ** 0.5
    if norm == 0:
        return pooled
    return [v / norm for v in pooled]


def text_cache_key(text: str, model: str) -> str:
    """Deterministic cache key for an embedding (content hash + model)."""
    digest = hashlib.sha256(f"{model}:{text}".encode()).hexdigest()
    return f"duka:embed:{digest}"


class EmbeddingClient:
    """Interface to Ollama for local multilingual embeddings (FREE).

    Uses a multilingual model (default bge-m3) so Amharic and English content
    embed into the same semantic space — critical for an Amharic-first system.
    Long texts are chunked and mean-pooled into a single article vector.
    """

    def __init__(
        self,
        base_url: str = "http://ollama-embedding:11434",
        model: str = "bge-m3",
        redis_url: str = "",
        cache_ttl_seconds: int = 7 * 24 * 3600,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.client = httpx.AsyncClient(timeout=120)
        self.redis_url = redis_url
        self.cache_ttl_seconds = cache_ttl_seconds
        self._redis = None  # lazy; None until first successful connect

    async def close(self) -> None:
        await self.client.aclose()
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:
                pass
            self._redis = None

    async def _cache_get(self, key: str) -> Optional[List[float]]:
        if not self.redis_url:
            return None
        try:
            if self._redis is None:
                import redis.asyncio as aioredis

                self._redis = aioredis.from_url(self.redis_url, decode_responses=True)
            raw = await self._redis.get(key)
            if not raw:
                return None
            import json

            return json.loads(raw)
        except Exception:
            return None

    async def _cache_set(self, key: str, vector: List[float]) -> None:
        if not self.redis_url or self._redis is None:
            return
        try:
            import json

            await self._redis.set(key, json.dumps(vector), ex=self.cache_ttl_seconds)
        except Exception:
            pass

    async def is_available(self) -> bool:
        """Check if Ollama is running"""
        try:
            response = await self.client.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Ollama not available: {e}")
            return False

    async def pull_model(self) -> bool:
        """Pull embedding model from Ollama"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/pull",
                json={"name": self.model, "stream": False},
                timeout=300
            )
            if response.status_code == 200:
                logger.info(f"Successfully pulled model: {self.model}")
                return True
            else:
                logger.error(f"Failed to pull model: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error pulling model: {e}")
            return False

    async def _embed_single(self, text: str) -> Optional[List[float]]:
        """One embedding call for a short text."""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/embed",
                json={"model": self.model, "input": text},
                timeout=120
            )

            if response.status_code == 200:
                data = response.json()
                embeddings = data.get("embeddings", [])
                if embeddings:
                    return embeddings[0]  # First (only) embedding
            else:
                logger.error(f"Embedding error: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            return None

    async def embed(self, text: str) -> Optional[List[float]]:
        """Generate an article embedding.

        Long texts are split into overlapping chunks, each embedded separately,
        then mean-pooled (L2-normalized) into a single vector. Results are
        cached in Redis by content hash so re-analysis of identical text (the
        dedup layer runs upstream) skips the model entirely.
        """
        if not text or len(text.strip()) < 10:
            return None

        cache_key = text_cache_key(text.strip(), self.model)
        cached = await self._cache_get(cache_key)
        if cached:
            return cached

        chunks = _chunk_text(text)
        if not chunks:
            return None

        if len(chunks) == 1:
            vector = await self._embed_single(chunks[0])
        else:
            chunk_vectors: List[List[float]] = []
            for chunk in chunks:
                vec = await self._embed_single(chunk)
                if vec is None:
                    return None  # partial embedding is worse than none
                chunk_vectors.append(vec)
            vector = _mean_pool(chunk_vectors)

        if vector:
            await self._cache_set(cache_key, vector)
        return vector


class VectorDBClient:
    """Interface to Qdrant Vector DB (FREE)"""

    def __init__(self, base_url: str = "http://qdrant:6333", collection_name: str = "duka_articles"):
        self.base_url = base_url.rstrip("/")
        self.collection_name = collection_name
        self.client = httpx.AsyncClient(timeout=120)

    async def close(self) -> None:
        await self.client.aclose()

    async def is_available(self) -> bool:
        """Check if Qdrant is running"""
        try:
            # Qdrant exposes its liveness probe at /healthz.  The older
            # /health path returns 404 on current Qdrant releases, which
            # incorrectly caused the LLM worker to disable RAG.
            response = await self.client.get(f"{self.base_url}/healthz", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Qdrant not available: {e}")
            return False

    async def create_collection(self, vector_size: int = 1024) -> bool:
        """Create collection if it doesn't exist.

        Default vector_size 1024 matches bge-m3. nomic-embed-text is 768 —
        pass the right size for the configured model (EMBEDDING_VECTOR_SIZE).
        """
        try:
            # Check if collection exists
            response = await self.client.get(
                f"{self.base_url}/collections/{self.collection_name}"
            )

            if response.status_code == 200:
                existing = (response.json() or {}).get("result", {}) or {}
                size = ((existing.get("config") or {}).get("params") or {}).get(
                    "vectors_size"
                )
                if size and size != vector_size:
                    logger.error(
                        "Qdrant collection '%s' has vector size %s but the "
                        "embedding model produces %s — delete the collection to "
                        "migrate to the new embedding model",
                        self.collection_name, size, vector_size,
                    )
                    return False
                logger.info(f"Collection '{self.collection_name}' already exists")
                return True

            # Create collection
            response = await self.client.put(
                f"{self.base_url}/collections/{self.collection_name}",
                json={
                    "vectors": {
                        "size": vector_size,
                        "distance": "Cosine"
                    }
                }
            )

            if response.status_code in (200, 201):
                logger.info(f"Created collection: {self.collection_name} (size={vector_size})")
                return True
            else:
                logger.error(f"Failed to create collection: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error creating collection: {e}")
            return False

    async def store_embedding(
        self,
        item_id: str,
        embedding: List[float],
        metadata: dict,
        vector_size: int = 1024,
    ) -> bool:
        """Store embedding in Qdrant"""
        try:
            await self.create_collection(vector_size=vector_size)
            response = await self.client.put(
                f"{self.base_url}/collections/{self.collection_name}/points",
                json={
                    "points": [
                        {
                            # A deterministic UUID avoids Python's randomized hash values
                            # and prevents collisions between worker restarts.
                            "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"duka-article:{item_id}")),
                            "vector": embedding,
                            "payload": {
                                "item_id": item_id,
                                "url": metadata.get("url", ""),
                                "job_id": metadata.get("job_id", ""),
                                "language": metadata.get("language", "en"),
                                "category": metadata.get("category", ""),
                                "text_summary": metadata.get("text_summary", "")[:500]  # Store summary for reference
                            }
                        }
                    ]
                }
            )

            if response.status_code in (200, 201):
                return True
            else:
                logger.error(f"Failed to store embedding: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error storing embedding: {e}")
            return False

    async def search_similar(
        self,
        embedding: List[float],
        top_k: int = 5,
        language_filter: str = None,
        category_filter: str = None,
        exclude_item_id: str = None,
        score_threshold: float = 0.0,
    ) -> List[dict]:
        """Search for similar articles in Qdrant.

        ``exclude_item_id`` MUST be the current item on re-analysis so a
        document never retrieves itself as its own RAG context.
        ``score_threshold`` drops weak matches (cosine similarity floor).
        """
        try:
            query = {
                "vector": embedding,
                "limit": top_k,
                "with_payload": True,
            }

            must = []
            must_not = []

            # Add language filter if specified
            if language_filter:
                must.append({
                    "key": "language",
                    "match": {"value": language_filter}
                })

            if category_filter:
                must.append({
                    "key": "category",
                    "match": {"value": category_filter}
                })

            # Self-exclusion: never retrieve the item being analyzed.
            if exclude_item_id:
                must_not.append({
                    "key": "item_id",
                    "match": {"value": exclude_item_id}
                })

            if must:
                query["filter"] = {"must": must}
            if must_not:
                query.setdefault("filter", {})["must_not"] = must_not

            response = await self.client.post(
                f"{self.base_url}/collections/{self.collection_name}/points/search",
                json=query
            )

            if response.status_code == 200:
                results = response.json().get("result", [])
                similar_articles = []

                for result in results:
                    score = result.get("score", 0)
                    if score_threshold and score < score_threshold:
                        continue
                    similar_articles.append({
                        "item_id": result["payload"].get("item_id", ""),
                        "url": result["payload"].get("url", ""),
                        "language": result["payload"].get("language", ""),
                        "category": result["payload"].get("category", ""),
                        "text_summary": result["payload"].get("text_summary", ""),
                        "score": score
                    })

                return similar_articles
            else:
                logger.error(f"Search failed: {response.text}")
                return []

        except Exception as e:
            logger.error(f"Error searching similar articles: {e}")
            return []


def format_similar_articles_context(similar_articles: List[dict]) -> str:
    """Format similar articles for LLM context"""
    if not similar_articles:
        return ""

    context = "SIMILAR ARTICLES FOR REFERENCE:\n"
    context += "=" * 50 + "\n\n"

    for i, article in enumerate(similar_articles[:3], 1):  # Top 3 only
        context += f"{i}. Item ID: {article['item_id']}\n"
        context += f"   URL: {article['url']}\n"
        context += f"   Language: {article['language']}\n"
        context += f"   Category: {article['category']}\n"
        context += f"   Summary: {article['text_summary'][:200]}...\n"
        context += f"   Similarity Score: {article['score']:.2f}\n\n"

    return context
