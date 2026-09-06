"""Redis-backed fixed-window API rate limiting."""

from __future__ import annotations

from fastapi import HTTPException, Request, status
from redis.asyncio import Redis

from app.common.config.settings import settings


async def enforce_rate_limit(
    request: Request,
    *,
    scope: str,
    limit: int,
    window_seconds: int,
) -> None:
    """Raise 429 when one client exceeds a security-sensitive endpoint limit."""
    client_ip = request.client.host if request.client else "unknown"
    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        key = f"api:rate:{scope}:{client_ip}"
        count = await redis.incr(key)
        if count == 1:
            await redis.expire(key, window_seconds)
        if count > limit:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many requests. Try again later.",
            )
    finally:
        await redis.aclose()
