import secrets
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

from app.config import settings
from app.models import ShortenRequest, ShortenResponse
from app.storage import URLRecord, store

app = FastAPI(title=settings.app_name)

# Prometheus counters and gauges — defined at module level so they persist across requests
urls_created_total = Counter("snaplink_urls_created_total", "Total number of URLs shortened")
redirects_total = Counter("snaplink_redirects_total", "Total number of redirects served")
urls_stored = Gauge("snaplink_urls_stored", "Current number of URLs in storage")


# NOTE: Fixed paths (/healthz, /readyz, /metrics) MUST be defined before /{code}.
# FastAPI matches routes in definition order, so /{code} would swallow them otherwise.


@app.get("/healthz", tags=["ops"])
async def healthz() -> dict[str, str]:
    """Liveness probe — just confirms the process is alive."""
    return {"status": "ok"}


@app.get("/readyz", tags=["ops"])
async def readyz() -> dict[str, str]:
    """Readiness probe — in milestone 1, storage is in-memory so always ready."""
    return {"status": "ok", "storage": "in-memory"}


@app.get("/metrics", tags=["ops"])
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/shorten", response_model=ShortenResponse, status_code=201, tags=["urls"])
async def shorten_url(body: ShortenRequest) -> ShortenResponse:
    """Accept a long URL and return a short code."""
    code = secrets.token_urlsafe(8)  # ~11 URL-safe chars, 64 bits of entropy
    record = URLRecord(
        code=code,
        original_url=str(body.url),
        created_at=datetime.now(UTC),
    )
    store.save(record)
    urls_created_total.inc()
    urls_stored.set(store.total())
    return ShortenResponse(code=code, short_url=f"{settings.base_url}/{code}")


@app.get("/{code}", tags=["urls"])
async def redirect(code: str) -> RedirectResponse:
    """Look up a short code and redirect to the original URL."""
    record = store.get(code)
    if record is None:
        raise HTTPException(status_code=404, detail="Short URL not found")
    store.increment_hits(code)
    redirects_total.inc()
    return RedirectResponse(url=record.original_url, status_code=302)
