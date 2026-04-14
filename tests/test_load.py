"""Tests for M3 – Load module (uses an in-memory DuckDB)."""

import pandas as pd
from pathlib import Path
from src.load import (
    get_duck_conn,
    load_table,
    load_table_dedup,
    stage_to_parquet,
    load_from_parquet,
    list_tables,
)


def test_load_and_list():
    conn = get_duck_conn(":memory:")
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    load_table(conn, df, "test_table", mode="replace")

    result = conn.execute("SELECT * FROM test_table").fetchdf()
    assert len(result) == 3

    conn.close()


def test_append_mode():
    conn = get_duck_conn(":memory:")
    df = pd.DataFrame({"x": [1]})
    load_table(conn, df, "tbl", mode="replace")
    load_table(conn, df, "tbl", mode="append")

    count = conn.execute("SELECT COUNT(*) FROM tbl").fetchone()[0]
    assert count == 2

    conn.close()


def test_dedup_initial_load():
    """First load with dedup should create the table."""
    conn = get_duck_conn(":memory:")
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    load_table_dedup(conn, df, "dedup_tbl", primary_key="id")

    result = conn.execute("SELECT * FROM dedup_tbl ORDER BY id").fetchdf()
    assert len(result) == 3
    conn.close()


def test_dedup_upsert():
    """Re-loading with overlapping PKs should update, not duplicate."""
    conn = get_duck_conn(":memory:")

    # Initial load
    df1 = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    load_table_dedup(conn, df1, "dedup_tbl", primary_key="id")

    # Upsert with updated row id=2 and new row id=4
    df2 = pd.DataFrame({"id": [2, 4], "val": ["B_updated", "d"]})
    load_table_dedup(conn, df2, "dedup_tbl", primary_key="id")

    result = conn.execute(
        "SELECT * FROM dedup_tbl ORDER BY id"
    ).fetchdf()

    assert len(result) == 4  # 1, 2(updated), 3, 4
    assert result.loc[result["id"] == 2, "val"].iloc[0] == "B_updated"
    conn.close()


def test_parquet_staging(tmp_path, monkeypatch):
    """Stage to Parquet and load back into DuckDB."""
    import src.load as load_module
    monkeypatch.setattr(load_module, "STAGING_DIR", tmp_path)

    df = pd.DataFrame({"x": [10, 20, 30], "y": ["a", "b", "c"]})
    pq_path = stage_to_parquet(df, "staged_tbl")
    assert pq_path.exists()

    conn = get_duck_conn(":memory:")
    load_from_parquet(conn, pq_path, "staged_tbl", mode="replace")

    result = conn.execute("SELECT * FROM staged_tbl").fetchdf()
    assert len(result) == 3
    conn.close()
