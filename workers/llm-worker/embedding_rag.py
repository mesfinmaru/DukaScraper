"""
RAG Embedding & Vector DB Module for llm-worker

Handles:
1. Embedding texts using Ollama (local, free)
2. Storing embeddings in Qdrant Vector DB
3. Retrieving similar articles for context
"""

import httpx
import json
import logging
import uuid
from typing import Optional, List

logger = logging.getLogger(__name__)


class EmbeddingClient:
    """Interface to Ollama for local embeddings (FREE)"""
    
    def __init__(self, base_url: str = "http://ollama-embedding:11434", model: str = "nomic-embed-text"):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.client = httpx.AsyncClient(timeout=120)
    
    async def close(self) -> None:
        await self.client.aclose()
    
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
    
    async def embed(self, text: str) -> Optional[List[float]]:
        """Generate embedding for text"""
        if not text or len(text.strip()) < 10:
            return None
        
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
    
    async def create_collection(self, vector_size: int = 768) -> bool:
        """Create collection if it doesn't exist"""
        try:
            # Check if collection exists
            response = await self.client.get(
                f"{self.base_url}/collections/{self.collection_name}"
            )
            
            if response.status_code == 200:
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
                logger.info(f"Created collection: {self.collection_name}")
                return True
            else:
                logger.error(f"Failed to create collection: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating collection: {e}")
            return False
    
    async def store_embedding(self, item_id: str, embedding: List[float], metadata: dict) -> bool:
        """Store embedding in Qdrant"""
        try:
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
    
    async def search_similar(self, embedding: List[float], top_k: int = 5, language_filter: str = None) -> List[dict]:
        """Search for similar articles in Qdrant"""
        try:
            query = {
                "vector": embedding,
                "limit": top_k,
                "with_payload": True
            }
            
            # Add language filter if specified
            if language_filter:
                query["filter"] = {
                    "must": [
                        {
                            "key": "language",
                            "match": {"value": language_filter}
                        }
                    ]
                }
            
            response = await self.client.post(
                f"{self.base_url}/collections/{self.collection_name}/points/search",
                json=query
            )
            
            if response.status_code == 200:
                results = response.json().get("result", [])
                similar_articles = []
                
                for result in results:
                    similar_articles.append({
                        "item_id": result["payload"].get("item_id", ""),
                        "url": result["payload"].get("url", ""),
                        "language": result["payload"].get("language", ""),
                        "category": result["payload"].get("category", ""),
                        "text_summary": result["payload"].get("text_summary", ""),
                        "score": result.get("score", 0)
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