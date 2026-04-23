"""
M3 – Load Module
────────────────
Persist transformed Pandas DataFrames into a local DuckDB analytical
database with optional Parquet intermediate staging and primary-key
based deduplication.

Phase 3 of the three-phase pipeline:
  Extract → Transform → Load (DataFrames → DuckDB)
"""

import math
import os
import logging
from datetime import date
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


# ── SCD Type 2 Load ─────────────────────────────────

def _vals_equal(a, b) -> bool:
    """Normalised equality check for SCD2 change detection.

    Handles:
    - NaN == NaN (both float NaN → equal, no false-positive change)
    - None == None (both null → equal)
    - None vs NaN cross-type (both are "null" → equal)
    - np.bool_ and np.integer types (not just Python bool/int)
    - bool/int coercion (True == 1, False == 0 → equal)
    - everything else uses standard equality
    """
    a_nan = a is None or (isinstance(a, float) and math.isnan(a))
    b_nan = b is None or (isinstance(b, float) and math.isnan(b))
    if a_nan and b_nan:
        return True
    try:
        import numpy as np
        if isinstance(a, (bool, np.bool_)) and isinstance(b, (bool, np.bool_)):
            return bool(a) == bool(b)
    except ImportError:
        if isinstance(a, bool) and isinstance(b, bool):
            return bool(a) == bool(b)
    return a == b


def load_scd2(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
    natural_key: str,
    tracked_cols: list[str],
    surrogate_key_col: str | None = None,
    today: date | None = None,
) -> None:
    """
    SCD Type 2 upsert: tracks history for tracked_cols, expires old rows,
    inserts new versions. Assigns surrogate keys if surrogate_key_col is given.

    Non-tracked columns are Type 1 (overwritten in place for unchanged rows).
    """
    from datetime import timedelta
    today = today or date.today()
    yesterday = today - timedelta(days=1)

    existing = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]

    if table_name not in existing:
        init = df.copy()
        if surrogate_key_col and surrogate_key_col not in init.columns:
            init.insert(0, surrogate_key_col, range(1, len(init) + 1))
        init["effective_from"] = pd.Timestamp(today)
        init["effective_to"]   = pd.NaT
        init["is_current"]     = True
        init["version"]        = 1
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM init")
        log.info("  %s → initial SCD2 load: %d rows", table_name, len(df))
        return

    current = conn.execute(
        f"SELECT * FROM {table_name} WHERE is_current = TRUE"
    ).fetchdf()

    incoming_keys = set(df[natural_key].tolist())
    current_keys  = set(current[natural_key].tolist())
    new_keys      = incoming_keys - current_keys
    candidate_keys = incoming_keys & current_keys

    changed_keys = []
    for key in candidate_keys:
        inc_row = df[df[natural_key] == key].iloc[0]
        cur_row = current[current[natural_key] == key].iloc[0]
        if any(not _vals_equal(inc_row[c], cur_row[c]) for c in tracked_cols):
            changed_keys.append(key)

    # 1. Expire changed rows
    changed_keys_set = set(changed_keys)
    for key in changed_keys:
        conn.execute(f"""
            UPDATE {table_name}
            SET effective_to = DATE '{yesterday.isoformat()}', is_current = FALSE
            WHERE "{natural_key}" = {key!r} AND is_current = TRUE
        """)

    # 2. Insert new versions for changed rows + new rows
    keys_to_insert = changed_keys_set | new_keys
    if keys_to_insert:
        rows = df[df[natural_key].isin(keys_to_insert)].copy()
        rows["effective_from"] = pd.Timestamp(today)
        rows["effective_to"]   = pd.NaT
        rows["is_current"]     = True

        versions = []
        for _, row in rows.iterrows():
            key = row[natural_key]
            if key in changed_keys_set:
                old_v = int(current[current[natural_key] == key]["version"].iloc[0])
                versions.append(old_v + 1)
            else:
                versions.append(1)
        rows["version"] = versions

        if surrogate_key_col:
            max_sk = conn.execute(
                f'SELECT COALESCE(MAX("{surrogate_key_col}"), 0) FROM {table_name}'
            ).fetchone()[0]
            rows[surrogate_key_col] = range(int(max_sk) + 1,
                                            int(max_sk) + 1 + len(rows))

        # Align column order to match the existing table schema
        table_cols = conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        ).fetchdf()["column_name"].tolist()
        rows = rows[[c for c in table_cols if c in rows.columns]]

        conn.execute(f"INSERT INTO {table_name} SELECT * FROM rows")

    # 3. Type 1 overwrite for non-tracked columns on UNCHANGED rows
    unchanged_keys = candidate_keys - changed_keys_set
    scd_meta_cols  = {"effective_from", "effective_to", "is_current", "version"}
    type1_cols     = [
        c for c in df.columns
        if c not in tracked_cols
        and c != natural_key
        and c not in scd_meta_cols
        and (surrogate_key_col is None or c != surrogate_key_col)
    ]
    for key in unchanged_keys:
        inc_row = df[df[natural_key] == key].iloc[0]
        for col in type1_cols:
            val = inc_row[col]
            if val is None or (isinstance(val, float) and math.isnan(val)):
                val_repr = "NULL"
            elif isinstance(val, str):
                val_repr = "'" + val.replace("'", "''") + "'"
            else:
                val_repr = repr(val.item() if hasattr(val, "item") else val)
            conn.execute(f"""
                UPDATE {table_name}
                SET "{col}" = {val_repr}
                WHERE "{natural_key}" = {key!r} AND is_current = TRUE
            """)

    log.info(
        "  %s SCD2: %d changed, %d new, %d unchanged (Type1 cols: %s)",
        table_name, len(changed_keys), len(new_keys),
        len(candidate_keys) - len(changed_keys), type1_cols,
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
