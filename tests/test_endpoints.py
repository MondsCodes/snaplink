"""
Unit tests for SnapLink milestone 2 endpoints.

Database is an in-memory SQLite instance via aiosqlite — no external services needed.
The get_db dependency is overridden so each test gets a fresh session backed by
the same engine; tables are (re)created once per module.
"""
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import get_db
from app.db_models import Base, Url
from app.main import app

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="module")
async def engine():
    eng = create_async_engine(TEST_DB_URL)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest.fixture
async def db_session(engine) -> AsyncGenerator[AsyncSession, None]:  # type: ignore[override]
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
        # Roll back any failed flush before attempting cleanup so the session
        # is in a usable state even if the test triggered a DB error.
        await session.rollback()
        for table in reversed(Base.metadata.sorted_tables):
            await session.execute(table.delete())
        await session.commit()


@pytest.fixture
async def client(db_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    async def _override() -> AsyncGenerator[AsyncSession, None]:
        yield db_session

    app.dependency_overrides[get_db] = _override
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


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


async def test_shorten_short_url_contains_base_url(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    # Default base_url is http://localhost:8000
    assert resp.json()["short_url"].startswith("http://localhost:8000/")


async def test_shorten_rejects_invalid_url(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "not-a-url"})
    assert resp.status_code == 422


async def test_shorten_rejects_missing_url_field(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={})
    assert resp.status_code == 422


async def test_shorten_rejects_empty_body(client: AsyncClient) -> None:
    resp = await client.post("/shorten", content=b"", headers={"content-type": "application/json"})
    assert resp.status_code == 422


async def test_shorten_persists_url(client: AsyncClient, db_session: AsyncSession) -> None:
    await client.post("/shorten", json={"url": "https://example.com"})
    result = await db_session.execute(select(Url))
    rows = result.scalars().all()
    assert len(rows) == 1
    assert rows[0].original_url == "https://example.com/"


async def test_shorten_multiple_calls_produce_unique_codes(client: AsyncClient) -> None:
    codes = set()
    for _ in range(10):
        resp = await client.post("/shorten", json={"url": "https://example.com"})
        codes.add(resp.json()["code"])
    assert len(codes) == 10, "expected all generated codes to be unique"


async def test_shorten_code_is_url_safe(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp.json()["code"]
    # URL-safe base64 alphabet: A-Z a-z 0-9 - _
    assert all(c.isalnum() or c in "-_" for c in code)


async def test_shorten_preserves_url_with_query_params(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    target = "https://example.com/path?foo=bar&baz=qux"
    await client.post("/shorten", json={"url": target})
    result = await db_session.execute(select(Url))
    row = result.scalars().first()
    assert row is not None
    assert row.original_url == target


# ---------------------------------------------------------------------------
# GET /{code}
# ---------------------------------------------------------------------------


async def test_redirect_issues_302(client: AsyncClient) -> None:
    resp_shorten = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp_shorten.json()["code"]
    resp = await client.get(f"/{code}", follow_redirects=False)
    assert resp.status_code == 302


async def test_redirect_location_matches_original_url(client: AsyncClient) -> None:
    resp_shorten = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp_shorten.json()["code"]
    resp = await client.get(f"/{code}", follow_redirects=False)
    # Pydantic normalises https://example.com → https://example.com/
    assert resp.headers["location"] in ("https://example.com", "https://example.com/")


async def test_redirect_unknown_code_returns_404(client: AsyncClient) -> None:
    resp = await client.get("/doesnotexist", follow_redirects=False)
    assert resp.status_code == 404


async def test_redirect_404_body_contains_detail(client: AsyncClient) -> None:
    resp = await client.get("/doesnotexist", follow_redirects=False)
    assert "detail" in resp.json()


async def test_redirect_preserves_url_with_query_params(client: AsyncClient) -> None:
    target = "https://example.com/path?a=1&b=2"
    shorten_resp = await client.post("/shorten", json={"url": target})
    code = shorten_resp.json()["code"]
    redirect_resp = await client.get(f"/{code}", follow_redirects=False)
    assert redirect_resp.headers["location"] == target


async def test_redirect_follows_through_to_destination(client: AsyncClient) -> None:
    """With follow_redirects=True the client should land on the original URL."""
    shorten_resp = await client.post("/shorten", json={"url": "https://example.com"})
    code = shorten_resp.json()["code"]
    # follow_redirects=True — httpx will follow the 302 (network call is intercepted by ASGI)
    redirect_resp = await client.get(f"/{code}", follow_redirects=True)
    # The final URL in the redirect chain should be the original
    assert str(redirect_resp.url) in ("https://example.com", "https://example.com/")


# ---------------------------------------------------------------------------
# GET /healthz
# ---------------------------------------------------------------------------


async def test_healthz_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200


async def test_healthz_body(client: AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.json() == {"status": "ok"}


async def test_healthz_no_db_dependency(client: AsyncClient) -> None:
    """Liveness probe must respond even if the DB override is cleared."""
    app.dependency_overrides.clear()
    resp = await client.get("/healthz")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /readyz
# ---------------------------------------------------------------------------


async def test_readyz_returns_200_with_working_db(client: AsyncClient) -> None:
    resp = await client.get("/readyz")
    assert resp.status_code == 200


async def test_readyz_body_reports_postgres_storage(client: AsyncClient) -> None:
    resp = await client.get("/readyz")
    data = resp.json()
    assert data["status"] == "ok"
    assert data["storage"] == "postgres"


async def test_readyz_returns_503_when_db_unavailable() -> None:
    """readyz must return 503 when the database cannot be reached."""

    async def broken_db() -> AsyncGenerator[AsyncSession, None]:
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute.side_effect = OperationalError(
            "connection refused", params=None, orig=None
        )
        yield mock_session

    app.dependency_overrides[get_db] = broken_db
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            resp = await c.get("/readyz")
        assert resp.status_code == 503
        assert "database" in resp.json()["detail"]
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# GET /metrics
# ---------------------------------------------------------------------------


async def test_metrics_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/metrics")
    assert resp.status_code == 200


async def test_metrics_content_type_is_prometheus(client: AsyncClient) -> None:
    resp = await client.get("/metrics")
    assert "text/plain" in resp.headers["content-type"]


async def test_metrics_contains_snaplink_counters(client: AsyncClient) -> None:
    await client.post("/shorten", json={"url": "https://example.com"})
    resp = await client.get("/metrics")
    assert "snaplink_urls_created_total" in resp.text
    assert "snaplink_redirects_total" in resp.text


async def test_metrics_shorten_counter_increments(client: AsyncClient) -> None:
    before = await client.get("/metrics")
    before_count = _parse_counter(before.text, "snaplink_urls_created_total")

    await client.post("/shorten", json={"url": "https://example.com"})

    after = await client.get("/metrics")
    after_count = _parse_counter(after.text, "snaplink_urls_created_total")

    assert after_count == before_count + 1


async def test_metrics_redirects_counter_increments(client: AsyncClient) -> None:
    shorten_resp = await client.post("/shorten", json={"url": "https://example.com"})
    code = shorten_resp.json()["code"]

    before = await client.get("/metrics")
    before_count = _parse_counter(before.text, "snaplink_redirects_total")

    await client.get(f"/{code}", follow_redirects=False)

    after = await client.get("/metrics")
    after_count = _parse_counter(after.text, "snaplink_redirects_total")

    assert after_count == before_count + 1


def _parse_counter(metrics_text: str, name: str) -> float:
    """Pull the numeric value of a Prometheus counter out of the exposition text."""
    for line in metrics_text.splitlines():
        if line.startswith(name) and not line.startswith("#"):
            return float(line.split()[-1])
    raise ValueError(f"counter {name!r} not found in metrics output")
