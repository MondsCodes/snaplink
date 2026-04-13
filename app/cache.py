from collections.abc import AsyncGenerator

from redis.asyncio import Redis

from app.config import settings

CACHE_TTL = 3600  # 1 hour
_KEY_PREFIX = "url:"


async def get_redis() -> AsyncGenerator[Redis, None]:
    client: Redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


async def cache_get(redis: Redis, code: str) -> str | None:
    value: str | None = await redis.get(f"{_KEY_PREFIX}{code}")
    return value


async def cache_set(redis: Redis, code: str, original_url: str) -> None:
    await redis.set(f"{_KEY_PREFIX}{code}", original_url, ex=CACHE_TTL)
