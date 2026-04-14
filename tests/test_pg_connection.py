"""Tests for PostgreSQL connectivity and extraction."""

import pytest
from sqlalchemy import text
from src.extract import get_pg_engine, list_tables


@pytest.fixture(scope="module")
def engine():
    """Create a SQLAlchemy engine from .env credentials."""
    return get_pg_engine()


def test_pg_connection(engine):
    """Verify that PostgreSQL is reachable and responds."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
        assert result[0] == 1


def test_pg_version(engine):
    """Check PostgreSQL version is returned."""
    with engine.connect() as conn:
        row = conn.execute(text("SELECT version()")).fetchone()
        assert "PostgreSQL" in row[0]


def test_list_public_tables(engine):
    """List tables in the public schema (may be empty)."""
    tables = list_tables(engine)
    assert isinstance(tables, list)
