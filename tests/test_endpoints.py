"""
Endpoint tests for SnapLink milestone 1 (in-memory storage).

Each endpoint gets a unit test (shape/contract) and a behaviour test (integration-style,
exercising the full HTTP stack via httpx + ASGI transport).
"""
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.storage import store


@pytest.fixture(autouse=True)
def reset_store() -> None:
    """Wipe in-memory state before every test so tests don't bleed into each other."""
    store.clear()


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Async HTTP client wired directly to the ASGI app — no network needed."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# POST /shorten
# ---------------------------------------------------------------------------


async def test_shorten_returns_201(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    assert resp.status_code == 201


async def test_shorten_response_contains_code_and_short_url(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    data = resp.json()
    assert "code" in data
    assert "short_url" in data
    assert data["code"] in data["short_url"]


async def test_shorten_rejects_invalid_url(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "not-a-url"})
    assert resp.status_code == 422


async def test_shorten_stores_url(client: AsyncClient) -> None:
    await client.post("/shorten", json={"url": "https://example.com"})
    assert store.total() == 1


# ---------------------------------------------------------------------------
# GET /{code}
# ---------------------------------------------------------------------------


async def test_redirect_issues_302(client: AsyncClient) -> None:
    resp_shorten = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp_shorten.json()["code"]

    resp = await client.get(f"/{code}", follow_redirects=False)
    assert resp.status_code == 302


async def test_redirect_location_matches_original_url(client: AsyncClient) -> None:
    await client.post("/shorten", json={"url": "https://example.com"})
    code = store._data and next(iter(store._data))  # peek at the stored code

    resp = await client.get(f"/{code}", follow_redirects=False)
    # Pydantic normalises https://example.com → https://example.com/
    assert resp.headers["location"] in ("https://example.com", "https://example.com/")


async def test_redirect_unknown_code_returns_404(client: AsyncClient) -> None:
    resp = await client.get("/doesnotexist", follow_redirects=False)
    assert resp.status_code == 404


async def test_redirect_increments_hit_count(client: AsyncClient) -> None:
    resp_shorten = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp_shorten.json()["code"]

    await client.get(f"/{code}", follow_redirects=False)
    await client.get(f"/{code}", follow_redirects=False)

    record = store.get(code)
    assert record is not None
    assert record.hit_count == 2


# ---------------------------------------------------------------------------
# GET /healthz
# ---------------------------------------------------------------------------


async def test_healthz_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200


async def test_healthz_body(client: AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# GET /readyz
# ---------------------------------------------------------------------------


async def test_readyz_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/readyz")
    assert resp.status_code == 200


async def test_readyz_body(client: AsyncClient) -> None:
    resp = await client.get("/readyz")
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# GET /metrics
# ---------------------------------------------------------------------------


async def test_metrics_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/metrics")
    assert resp.status_code == 200


async def test_metrics_contains_snaplink_counters(client: AsyncClient) -> None:
    # Create a URL first so the counter is non-zero
    await client.post("/shorten", json={"url": "https://example.com"})
    resp = await client.get("/metrics")
    assert "snaplink_urls_created_total" in resp.text
    assert "snaplink_redirects_total" in resp.text
