"""create urls table

Revision ID: 0001
Revises:
Create Date: 2026-04-12
"""

import sqlalchemy as sa

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "urls",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("code", sa.String(12), nullable=False),
        sa.Column("original_url", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("hit_count", sa.BigInteger(), server_default="0", nullable=False),
    )
    op.create_index("ix_urls_code", "urls", ["code"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_urls_code", table_name="urls")
    op.drop_table("urls")
