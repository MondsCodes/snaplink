"""
Unit tests for SnapLink milestone 2 endpoints.

Database is an in-memory SQLite instance via aiosqlite — no external services needed.
The get_db dependency is overridden so each test gets a fresh session backed by
the same engine; tables are (re)created once per module.
"""
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
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


async def test_shorten_rejects_invalid_url(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "not-a-url"})
    assert resp.status_code == 422


async def test_shorten_persists_url(client: AsyncClient, db_session: AsyncSession) -> None:
    await client.post("/shorten", json={"url": "https://example.com"})
    result = await db_session.execute(select(Url))
    rows = result.scalars().all()
    assert len(rows) == 1
    assert rows[0].original_url == "https://example.com/"


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
# GET /metrics
# ---------------------------------------------------------------------------


async def test_metrics_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/metrics")
    assert resp.status_code == 200


async def test_metrics_contains_snaplink_counters(client: AsyncClient) -> None:
    await client.post("/shorten", json={"url": "https://example.com"})
    resp = await client.get("/metrics")
    assert "snaplink_urls_created_total" in resp.text
    assert "snaplink_redirects_total" in resp.text
