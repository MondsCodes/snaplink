import secrets
from typing import Annotated

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.responses import RedirectResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from redis.asyncio import Redis
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import cache_get, cache_set, get_redis
from app.config import settings
from app.database import AsyncSessionLocal, get_db
from app.db_models import Url
from app.models import ShortenRequest, ShortenResponse

app = FastAPI(title=settings.app_name)

# Prometheus counters — reset on process restart, which is acceptable for a portfolio project
urls_created_total = Counter("snaplink_urls_created_total", "Total number of URLs shortened")
redirects_total = Counter("snaplink_redirects_total", "Total number of redirects served")
cache_hits_total = Counter("snaplink_cache_hits_total", "Redirects served from Redis cache")
cache_misses_total = Counter("snaplink_cache_misses_total", "Redirects that required a DB lookup")


# NOTE: Fixed paths (/healthz, /readyz, /metrics) MUST be defined before /{code}.
# FastAPI matches routes in definition order, so /{code} would swallow them otherwise.


@app.get("/healthz", tags=["ops"])
async def healthz() -> dict[str, str]:
    """Liveness probe — just confirms the process is alive."""
    return {"status": "ok"}


@app.get("/readyz", tags=["ops"])
async def readyz(
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> dict[str, str]:
    """Readiness probe — checks that both Postgres and Redis are reachable."""
    errors: list[str] = []
    try:
        await db.execute(text("SELECT 1"))
    except Exception:
        errors.append("database")
    try:
        await redis.ping()  # type: ignore[misc]
    except Exception:
        errors.append("redis")
    if errors:
        raise HTTPException(
            status_code=503, detail=f"unavailable: {', '.join(errors)}"
        )
    return {"status": "ok", "storage": "postgres+redis"}


@app.get("/metrics", tags=["ops"])
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/shorten", response_model=ShortenResponse, status_code=201, tags=["urls"])
async def shorten_url(
    body: ShortenRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> ShortenResponse:
    """Accept a long URL and return a short code."""
    code = secrets.token_urlsafe(8)  # ~11 URL-safe chars, 64 bits of entropy
    url = Url(code=code, original_url=str(body.url))
    db.add(url)
    await db.commit()
    urls_created_total.inc()
    return ShortenResponse(code=code, short_url=f"{settings.base_url}/{code}")


async def _increment_hits(code: str) -> None:
    """Background task: increment hit_count without blocking the redirect.

    Failures are silently dropped — a missed counter increment is preferable
    to surfacing a DB error to the user after they've already been redirected.
    """
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(Url).where(Url.code == code).values(hit_count=Url.hit_count + 1)
            )
            await session.commit()
    except Exception:
        pass


@app.get("/{code}", tags=["urls"])
async def redirect(
    code: str,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> RedirectResponse:
    """Look up a short code and redirect to the original URL.

    Redis is checked first (1-hour TTL). On a cache miss the URL is fetched
    from Postgres and written back to Redis before redirecting.
    """
    original_url = await cache_get(redis, code)
    if original_url is not None:
        cache_hits_total.inc()
        background_tasks.add_task(_increment_hits, code)
        redirects_total.inc()
        return RedirectResponse(url=original_url, status_code=302)

    # Cache miss — query Postgres then back-fill the cache
    cache_misses_total.inc()
    result = await db.execute(select(Url).where(Url.code == code))
    url = result.scalar_one_or_none()
    if url is None:
        raise HTTPException(status_code=404, detail="Short URL not found")

    await cache_set(redis, code, url.original_url)
    background_tasks.add_task(_increment_hits, code)
    redirects_total.inc()
    return RedirectResponse(url=url.original_url, status_code=302)
