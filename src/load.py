"""
M3 – Load Module
────────────────
Persist transformed Pandas DataFrames into a local DuckDB analytical
database with optional Parquet intermediate staging and primary-key
based deduplication.

Phase 3 of the three-phase pipeline:
  Extract → Transform → Load (DataFrames → DuckDB)
"""

import os
import logging
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

from src.config import DUCKDB_PATH, STAGING_DIR, DIM_TABLE, FACT_TABLE, PK_MAP

load_dotenv()

log = logging.getLogger("databridge.load")


# ── DuckDB Connection ────────────────────────────────

def get_duck_conn(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Open (or create) a DuckDB database and return the connection."""
    path = db_path or DUCKDB_PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    return duckdb.connect(path)


# ── Parquet Staging ───────────────────────────────────

def stage_to_parquet(df: pd.DataFrame, table_name: str) -> Path:
    """
    Write a DataFrame to a Parquet file in the staging directory.

    Returns the path to the written file.  This mirrors the STG
    pattern used in production IBMi/Oracle ETL pipelines.
    """
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    path = STAGING_DIR / f"{table_name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    log.info("  Staged %s → %s (%d rows)", table_name, path, len(df))
    return path


def load_from_parquet(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    table_name: str,
    mode: str = "replace",
) -> None:
    """
    Load a Parquet file into DuckDB.

    Uses DuckDB's native ``read_parquet()`` for optimal columnar
    ingestion performance.
    """
    pq = str(parquet_path)
    if mode == "replace":
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{pq}')"
        )
    elif mode == "append":
        conn.execute(
            f"INSERT INTO {table_name} SELECT * FROM read_parquet('{pq}')"
        )
    else:
        raise ValueError(f"Unsupported mode: {mode!r}")


# ── Standard Load (DataFrame → DuckDB) ──────────────

def load_table(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
    mode: str = "replace",
) -> None:
    """
    Write a DataFrame to DuckDB.

    Parameters
    ----------
    mode : 'replace' | 'append'
        'replace' drops the table first; 'append' inserts rows.
    """
    if mode == "replace":
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    elif mode == "append":
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    else:
        raise ValueError(f"Unsupported mode: {mode!r}")


# ── Dedup Load (Primary-Key Aware) ───────────────────

def load_table_dedup(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
    primary_key: str | list[str],
) -> None:
    """
    Upsert a DataFrame into DuckDB using primary-key deduplication.

    For incremental loads this prevents duplicate rows:
      1. Delete existing rows whose PK matches the incoming data.
      2. Insert all incoming rows.

    If the target table does not exist yet, it is created from the
    DataFrame directly (initial load).

    Parameters
    ----------
    primary_key : str | list[str]
        Column name(s) forming the primary key.
    """
    pk_cols = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    # Check if target table exists
    existing = [
        r[0] for r in conn.execute("SHOW TABLES").fetchall()
    ]

    if table_name not in existing:
        log.info("  %s → table does not exist, creating from DataFrame", table_name)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        return

    # Build the DELETE condition on primary key(s)
    pk_conditions = " AND ".join(
        f'{table_name}."{pk}" = _incoming."{pk}"' for pk in pk_cols
    )

    # Delete existing rows that match incoming PKs
    conn.execute(f"""
        DELETE FROM {table_name}
        WHERE EXISTS (
            SELECT 1 FROM df AS _incoming
            WHERE {pk_conditions}
        )
    """)

    # Insert all incoming rows
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    log.info(
        "  %s → dedup-loaded %d rows (pk=%s)",
        table_name, len(df), pk_cols,
    )


# ── Star Schema Load (Phase 3) ───────────────────────

def load_star_schema(
    dim_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    db_path: str | None = None,
    progress_callback=None,
) -> None:
    """
    Load the star-schema tables (dim + fact) into DuckDB.

    Replaces any existing dim_account_customer and fact_transactions tables.
    """
    conn = get_duck_conn(db_path)

    if progress_callback:
        progress_callback("Loading dimension table", 1, 2)

    load_table(conn, dim_df, DIM_TABLE, mode="replace")
    log.info("  Loaded %s → %d rows", DIM_TABLE, len(dim_df))

    if progress_callback:
        progress_callback("Loading fact table", 2, 2)

    load_table(conn, fact_df, FACT_TABLE, mode="replace")
    log.info("  Loaded %s → %d rows", FACT_TABLE, len(fact_df))

    conn.close()
    log.info("Star schema loaded successfully into DuckDB")


# ── Batch Loaders ─────────────────────────────────────

def load_all(
    dataframes: dict[str, pd.DataFrame],
    db_path: str | None = None,
    mode: str = "replace",
) -> None:
    """Load every DataFrame in the dict into DuckDB."""
    conn = get_duck_conn(db_path)
    for name, df in dataframes.items():
        load_table(conn, df, name, mode=mode)
    conn.close()


def load_all_staged(
    dataframes: dict[str, pd.DataFrame],
    db_path: str | None = None,
    mode: str = "replace",
) -> None:
    """
    Stage each DataFrame to Parquet, then load into DuckDB.

    This two-step approach mirrors production STG workflows and gives
    DuckDB's columnar reader optimal input.
    """
    conn = get_duck_conn(db_path)
    for name, df in dataframes.items():
        pq_path = stage_to_parquet(df, name)
        load_from_parquet(conn, pq_path, name, mode=mode)
    conn.close()


def load_all_dedup(
    dataframes: dict[str, pd.DataFrame],
    pk_map: dict[str, str | list[str]],
    db_path: str | None = None,
) -> None:
    """
    Load DataFrames with primary-key deduplication.

    Parameters
    ----------
    pk_map : dict[str, str | list[str]]
        Mapping of ``{table_name: primary_key_column(s)}``.
        Tables not listed in pk_map fall back to ``mode='replace'``.
    """
    conn = get_duck_conn(db_path)
    for name, df in dataframes.items():
        if name in pk_map:
            load_table_dedup(conn, df, name, pk_map[name])
        else:
            load_table(conn, df, name, mode="replace")
    conn.close()


def list_tables(db_path: str | None = None) -> list[str]:
    """Return table names currently stored in DuckDB."""
    conn = get_duck_conn(db_path)
    tables = [
        row[0] for row in conn.execute("SHOW TABLES").fetchall()
    ]
    conn.close()
    return tables
