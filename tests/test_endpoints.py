"""
Unit tests for SnapLink milestone 3 endpoints.

Database: in-memory SQLite via aiosqlite.
Cache:    in-process FakeAsyncRedis — no external services needed.
Both dependencies are overridden per-test via FastAPI's dependency_overrides.
"""
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import fakeredis
import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.cache import CACHE_TTL, get_redis
from app.database import get_db
from app.db_models import Base, Url
from app.main import app

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


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
        await session.rollback()
        for table in reversed(Base.metadata.sorted_tables):
            await session.execute(table.delete())
        await session.commit()


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis, None]:  # type: ignore[type-arg]
    r: Redis = fakeredis.FakeAsyncRedis(decode_responses=True)  # type: ignore[type-arg]
    yield r
    await r.aclose()


@pytest.fixture
async def client(
    db_session: AsyncSession, redis_client: Redis  # type: ignore[type-arg]
) -> AsyncGenerator[AsyncClient, None]:
    async def _db() -> AsyncGenerator[AsyncSession, None]:
        yield db_session

    async def _redis() -> AsyncGenerator[Redis, None]:  # type: ignore[type-arg]
        yield redis_client

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_redis] = _redis
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
# GET /{code} — redirect + cache behaviour
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


async def test_first_redirect_is_a_cache_miss(
    client: AsyncClient, redis_client: Redis  # type: ignore[type-arg]
) -> None:
    """Before any redirect the key must not exist in Redis."""
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp.json()["code"]
    assert await redis_client.get(f"url:{code}") is None

    await client.get(f"/{code}", follow_redirects=False)

    assert await redis_client.get(f"url:{code}") is not None


async def test_cache_is_populated_with_correct_url(
    client: AsyncClient, redis_client: Redis  # type: ignore[type-arg]
) -> None:
    target = "https://example.com/path?x=1"
    resp = await client.post("/shorten", json={"url": target})
    code = resp.json()["code"]

    await client.get(f"/{code}", follow_redirects=False)

    cached = await redis_client.get(f"url:{code}")
    assert cached == target


async def test_cache_hit_serves_correct_url(
    client: AsyncClient, redis_client: Redis  # type: ignore[type-arg]
) -> None:
    """Pre-seed the cache and verify the redirect uses the cached value."""
    await redis_client.set("url:testcode", "https://cached.example.com", ex=CACHE_TTL)

    resp = await client.get("/testcode", follow_redirects=False)

    assert resp.status_code == 302
    assert resp.headers["location"] == "https://cached.example.com"


async def test_cache_hit_does_not_require_db(
    client: AsyncClient,
    redis_client: Redis,  # type: ignore[type-arg]
    db_session: AsyncSession,
) -> None:
    """When Redis has the URL, no DB row is needed to serve the redirect."""
    await redis_client.set("url:nodbcode", "https://nodbrequired.example.com", ex=CACHE_TTL)

    resp = await client.get("/nodbcode", follow_redirects=False)

    assert resp.status_code == 302
    result = await db_session.execute(select(Url).where(Url.code == "nodbcode"))
    assert result.scalar_one_or_none() is None


async def test_cache_ttl_is_set(
    client: AsyncClient, redis_client: Redis  # type: ignore[type-arg]
) -> None:
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp.json()["code"]
    await client.get(f"/{code}", follow_redirects=False)

    ttl = await redis_client.ttl(f"url:{code}")
    assert 0 < ttl <= CACHE_TTL


async def test_cache_miss_counter_increments(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp.json()["code"]

    before = _parse_counter((await client.get("/metrics")).text, "snaplink_cache_misses_total")
    await client.get(f"/{code}", follow_redirects=False)
    after = _parse_counter((await client.get("/metrics")).text, "snaplink_cache_misses_total")

    assert after == before + 1


async def test_cache_hit_counter_increments(
    client: AsyncClient, redis_client: Redis  # type: ignore[type-arg]
) -> None:
    await redis_client.set("url:hitme", "https://example.com", ex=CACHE_TTL)

    before = _parse_counter((await client.get("/metrics")).text, "snaplink_cache_hits_total")
    await client.get("/hitme", follow_redirects=False)
    after = _parse_counter((await client.get("/metrics")).text, "snaplink_cache_hits_total")

    assert after == before + 1


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
    app.dependency_overrides.clear()
    resp = await client.get("/healthz")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /readyz
# ---------------------------------------------------------------------------


async def test_readyz_returns_200_with_working_deps(client: AsyncClient) -> None:
    resp = await client.get("/readyz")
    assert resp.status_code == 200


async def test_readyz_body_reports_storage(client: AsyncClient) -> None:
    resp = await client.get("/readyz")
    data = resp.json()
    assert data["status"] == "ok"
    assert data["storage"] == "postgres+redis"


async def test_readyz_returns_503_when_db_unavailable() -> None:
    async def broken_db() -> AsyncGenerator[AsyncSession, None]:
        mock = AsyncMock(spec=AsyncSession)
        mock.execute.side_effect = OperationalError("conn refused", params=None, orig=None)
        yield mock

    async def fake_redis() -> AsyncGenerator[Redis, None]:  # type: ignore[type-arg]
        yield fakeredis.FakeAsyncRedis(decode_responses=True)  # type: ignore[type-arg]

    app.dependency_overrides[get_db] = broken_db
    app.dependency_overrides[get_redis] = fake_redis
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            resp = await c.get("/readyz")
        assert resp.status_code == 503
        assert "database" in resp.json()["detail"]
    finally:
        app.dependency_overrides.clear()


async def test_readyz_returns_503_when_redis_unavailable() -> None:
    async def fake_db() -> AsyncGenerator[AsyncSession, None]:
        mock = AsyncMock(spec=AsyncSession)
        yield mock

    async def broken_redis() -> AsyncGenerator[Redis, None]:  # type: ignore[type-arg]
        mock = AsyncMock(spec=Redis)
        mock.ping.side_effect = ConnectionError("redis down")
        yield mock  # type: ignore[misc]

    app.dependency_overrides[get_db] = fake_db
    app.dependency_overrides[get_redis] = broken_redis
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            resp = await c.get("/readyz")
        assert resp.status_code == 503
        assert "redis" in resp.json()["detail"]
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


async def test_metrics_contains_all_counters(client: AsyncClient) -> None:
    await client.post("/shorten", json={"url": "https://example.com"})
    resp = await client.get("/metrics")
    for name in (
        "snaplink_urls_created_total",
        "snaplink_redirects_total",
        "snaplink_cache_hits_total",
        "snaplink_cache_misses_total",
    ):
        assert name in resp.text, f"missing counter: {name}"


async def test_metrics_shorten_counter_increments(client: AsyncClient) -> None:
    before = _parse_counter((await client.get("/metrics")).text, "snaplink_urls_created_total")
    await client.post("/shorten", json={"url": "https://example.com"})
    after = _parse_counter((await client.get("/metrics")).text, "snaplink_urls_created_total")
    assert after == before + 1


async def test_metrics_redirects_counter_increments(client: AsyncClient) -> None:
    resp = await client.post("/shorten", json={"url": "https://example.com"})
    code = resp.json()["code"]
    before = _parse_counter((await client.get("/metrics")).text, "snaplink_redirects_total")
    await client.get(f"/{code}", follow_redirects=False)
    after = _parse_counter((await client.get("/metrics")).text, "snaplink_redirects_total")
    assert after == before + 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_counter(metrics_text: str, name: str) -> float:
    for line in metrics_text.splitlines():
        if line.startswith(name) and not line.startswith("#"):
            return float(line.split()[-1])
    raise ValueError(f"counter {name!r} not found in metrics output")
