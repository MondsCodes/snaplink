"""
Integration tests for SnapLink milestone 2.

Uses a real PostgreSQL instance via testcontainers. These tests exercise actual
SQL behaviour (constraints, indexing, concurrent hit-count increments) that
SQLite unit tests cannot cover.

Run with: pytest tests/test_integration.py
Requires Docker to be running.
"""
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from app.database import get_db
from app.db_models import Base, Url
from app.main import app


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:16") as container:
        yield container


@pytest.fixture(scope="module")
async def pg_engine(postgres_container: PostgresContainer):
    url = postgres_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql+asyncpg://"
    )
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_session(pg_engine) -> AsyncGenerator[AsyncSession, None]:
    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with factory() as session:
        yield session
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
# Core flow
# ---------------------------------------------------------------------------


async def test_shorten_and_redirect_roundtrip(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    assert resp.status_code == 201
    code = resp.json()["code"]

    redirect_resp = await client.get(f"/{code}", follow_redirects=False)
    assert redirect_resp.status_code == 302
    assert redirect_resp.headers["location"] in (
        "https://example.com",
        "https://example.com/",
    )


async def test_duplicate_codes_not_stored(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """The unique index on code must prevent duplicates."""
    await client.post("/shorten", json={"url": "https://example.com"})
    await client.post("/shorten", json={"url": "https://other.com"})
    result = await db_session.execute(select(Url))
    rows = result.scalars().all()
    codes = [r.code for r in rows]
    assert len(codes) == len(set(codes)), "duplicate codes detected in DB"


async def test_readyz_with_real_postgres(client: AsyncClient) -> None:
    resp = await client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "storage": "postgres"}


async def test_unknown_code_returns_404(client: AsyncClient) -> None:
    resp = await client.get("/doesnotexist", follow_redirects=False)
    assert resp.status_code == 404


async def test_original_url_preserved_exactly(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    target = "https://example.com/path?foo=bar&baz=qux"
    await client.post("/shorten", json={"url": target})
    result = await db_session.execute(select(Url))
    row = result.scalars().first()
    assert row is not None
    assert row.original_url == target


async def test_hit_count_incremented_after_redirect(
    client: AsyncClient, db_session: AsyncSession, pg_engine
) -> None:
    """Background task must increment hit_count in Postgres after a redirect."""
    import asyncio

    resp = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp.json()["code"]

    await client.get(f"/{code}", follow_redirects=False)
    await client.get(f"/{code}", follow_redirects=False)

    # Give background tasks a moment to complete
    await asyncio.sleep(0.1)

    # Bypass the overridden test session and query directly via the engine
    # so we see the committed result from the background task's own session.
    from sqlalchemy.ext.asyncio import async_sessionmaker

    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with factory() as s:
        result = await s.execute(select(Url).where(Url.code == code))
        row = result.scalar_one()
        assert row.hit_count == 2
