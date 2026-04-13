from datetime import datetime

from sqlalchemy import BigInteger, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# SQLite only autocrements INTEGER PRIMARY KEY, not BIGINT. Use with_variant so
# unit tests (SQLite) and production (Postgres) both work without schema changes.
_BigIntPK = BigInteger().with_variant(Integer, "sqlite")


class Url(Base):
    __tablename__ = "urls"

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(12), unique=True, index=True, nullable=False)
    original_url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False
    )
    hit_count: Mapped[int] = mapped_column(BigInteger, server_default="0", nullable=False)
