"""
M1 – Extract Module
───────────────────
Connects to PostgreSQL (OLTP) via SQLAlchemy and extracts
tables into Pandas DataFrames for downstream transformation.

Supports both full extraction and watermark-based incremental
extraction with high-watermark tracking via a local JSON file.

Phase 1 of the three-phase pipeline:
  Extract (PG → Parquet) → Transform → Load (DuckDB)
"""

import json
import os
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from src.config import (
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD,
    PG_SCHEMA, STAGING_DIR, WATERMARK_PATH,
)

load_dotenv()

log = logging.getLogger("databridge.extract")

# ── Constants ─────────────────────────────────────────
WATERMARK_FILE = WATERMARK_PATH


# ── PostgreSQL Engine ─────────────────────────────────

def get_pg_engine():
    """Build a SQLAlchemy engine from environment variables."""
    url = (
        f"postgresql+psycopg2://"
        f"{PG_USER}:{PG_PASSWORD}"
        f"@{PG_HOST}:{PG_PORT}"
        f"/{PG_DATABASE}"
    )
    return create_engine(url, pool_pre_ping=True)


def list_tables(engine) -> list[str]:
    """Return a list of user-defined table names in the configured schema."""
    query = text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_type = 'BASE TABLE'"
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(query, {"schema": PG_SCHEMA})]


# ── Full Extraction ───────────────────────────────────

def extract_table(engine, table_name: str) -> pd.DataFrame:
    """Extract a full table into a DataFrame."""
    return pd.read_sql_table(table_name, engine, schema=PG_SCHEMA)


def extract_all(engine) -> dict[str, pd.DataFrame]:
    """Extract every public table and return as {name: DataFrame}."""
    tables = list_tables(engine)
    return {t: extract_table(engine, t) for t in tables}


# ── Parquet Extraction (Phase 1) ──────────────────────

def extract_table_to_parquet(engine, table_name: str, output_dir: Path | None = None) -> Path:
    """
    Extract a single table from PostgreSQL and save as a Parquet file.

    Returns the path to the written Parquet file.
    """
    out = output_dir or STAGING_DIR
    out.mkdir(parents=True, exist_ok=True)

    df = extract_table(engine, table_name)
    parquet_path = out / f"{table_name}.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    log.info(
        "  Extracted %s → %s (%d rows, %d cols)",
        table_name, parquet_path, len(df), len(df.columns),
    )
    return parquet_path


def extract_tables_to_parquet(
    engine,
    table_names: list[str],
    output_dir: Path | None = None,
    progress_callback=None,
) -> dict[str, Path]:
    """
    Extract specific tables from PostgreSQL and write each to Parquet.

    Parameters
    ----------
    table_names : list[str]
        Only extract these tables (typically derived from the mapping sheet).
    output_dir : Path | None
        Override for the staging directory.
    progress_callback : callable | None
        Optional callback(table_name, index, total) for progress reporting.

    Returns
    -------
    dict[str, Path]
        Mapping of {table_name: parquet_file_path}.
    """
    out = output_dir or STAGING_DIR
    manifest: dict[str, Path] = {}

    total = len(table_names)
    for idx, table_name in enumerate(table_names, start=1):
        parquet_path = extract_table_to_parquet(engine, table_name, out)
        manifest[table_name] = parquet_path

        if progress_callback:
            progress_callback(table_name, idx, total)

    log.info("Extraction complete: %d tables → Parquet", len(manifest))
    return manifest


# ── Watermark Helpers ─────────────────────────────────

def _read_watermarks() -> dict:
    """Load high-watermark values from the JSON file."""
    if WATERMARK_FILE.exists():
        return json.loads(WATERMARK_FILE.read_text())
    return {}


def _write_watermarks(wm: dict) -> None:
    """Persist high-watermark values to the JSON file."""
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_FILE.write_text(json.dumps(wm, indent=2, default=str))


def get_watermark(table_name: str) -> str | None:
    """Return the last stored high-watermark for a table, or None."""
    return _read_watermarks().get(table_name)


def set_watermark(table_name: str, value) -> None:
    """Update the stored high-watermark for a table."""
    wm = _read_watermarks()
    wm[table_name] = str(value)
    _write_watermarks(wm)


# ── Incremental Extraction ────────────────────────────

def extract_incremental(
    engine,
    table_name: str,
    watermark_col: str,
    last_hwm: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    """
    Extract rows newer than the last high-watermark.

    Parameters
    ----------
    watermark_col : str
        Column used for change tracking (e.g. ``updated_at``).
    last_hwm : str | None
        Previous high-watermark value.  When *None*, a full extract is
        performed (equivalent to the initial load).

    Returns
    -------
    (DataFrame, new_hwm)
        The extracted data and the new high-watermark value (max of
        watermark_col), or *None* if the result is empty.
    """
    if last_hwm is None:
        log.info("  %s → no prior watermark, performing full extract", table_name)
        df = extract_table(engine, table_name)
    else:
        query = text(
            f'SELECT * FROM "{PG_SCHEMA}"."{table_name}" '
            f"WHERE \"{watermark_col}\" > :hwm "
            f"ORDER BY \"{watermark_col}\""
        )
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"hwm": last_hwm})
        log.info(
            "  %s → incremental extract (%d new rows since %s)",
            table_name, len(df), last_hwm,
        )

    # Compute the new high-watermark
    new_hwm = None
    if not df.empty and watermark_col in df.columns:
        new_hwm = str(df[watermark_col].max())

    return df, new_hwm


def extract_all_incremental(
    engine,
    watermark_map: dict[str, str],
) -> dict[str, pd.DataFrame]:
    """
    Incrementally extract tables using per-table watermark columns.

    Parameters
    ----------
    watermark_map : dict[str, str]
        Mapping of ``{table_name: watermark_column_name}``.
        Tables not listed fall back to full extraction.

    Returns
    -------
    dict[str, DataFrame]
    """
    tables = list_tables(engine)
    result: dict[str, pd.DataFrame] = {}

    for t in tables:
        if t in watermark_map:
            hwm = get_watermark(t)
            df, new_hwm = extract_incremental(
                engine, t, watermark_map[t], hwm
            )
            if new_hwm is not None:
                set_watermark(t, new_hwm)
            result[t] = df
        else:
            log.info("  %s → no watermark column configured, full extract", t)
            result[t] = extract_table(engine, t)

    return result
