import secrets
from typing import Annotated

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.responses import RedirectResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal, get_db
from app.db_models import Url
from app.models import ShortenRequest, ShortenResponse

app = FastAPI(title=settings.app_name)

# Prometheus counters — reset on process restart, which is acceptable for a portfolio project
urls_created_total = Counter("snaplink_urls_created_total", "Total number of URLs shortened")
redirects_total = Counter("snaplink_redirects_total", "Total number of redirects served")


# NOTE: Fixed paths (/healthz, /readyz, /metrics) MUST be defined before /{code}.
# FastAPI matches routes in definition order, so /{code} would swallow them otherwise.


@app.get("/healthz", tags=["ops"])
async def healthz() -> dict[str, str]:
    """Liveness probe — just confirms the process is alive."""
    return {"status": "ok"}


@app.get("/readyz", tags=["ops"])
async def readyz(db: Annotated[AsyncSession, Depends(get_db)]) -> dict[str, str]:
    """Readiness probe — checks that the database is reachable."""
    try:
        await db.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return {"status": "ok", "storage": "postgres"}


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
) -> RedirectResponse:
    """Look up a short code and redirect to the original URL."""
    result = await db.execute(select(Url).where(Url.code == code))
    url = result.scalar_one_or_none()
    if url is None:
        raise HTTPException(status_code=404, detail="Short URL not found")
    background_tasks.add_task(_increment_hits, code)
    redirects_total.inc()
    return RedirectResponse(url=url.original_url, status_code=302)
