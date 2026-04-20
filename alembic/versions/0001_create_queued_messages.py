"""create queued_messages table

Revision ID: 0001_create_queued_messages
Revises:
Create Date: 2026-04-20 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0001_create_queued_messages"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "queued_messages",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("is_delivered", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_queued_messages_topic", "queued_messages", ["topic"], unique=False)
    op.create_index(
        "ix_queued_messages_is_delivered",
        "queued_messages",
        ["is_delivered"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_queued_messages_is_delivered", table_name="queued_messages")
    op.drop_index("ix_queued_messages_topic", table_name="queued_messages")
    op.drop_table("queued_messages")
