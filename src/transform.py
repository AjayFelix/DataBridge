"""
M2 – Transform Module
─────────────────────
Mapping-sheet-driven transformations that build the star-schema
target tables from raw Parquet source files.

Phase 2 of the three-phase pipeline:
  Extract → Transform (Parquet → Star Schema) → Load

Also retains generic cleaning utilities for ad-hoc use.
"""

import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import openpyxl

log = logging.getLogger("databridge.transform")


# ══════════════════════════════════════════════════════
#  Calendar Dimension Builder
# ══════════════════════════════════════════════════════

def build_dim_date(parquet_dir: Path) -> pd.DataFrame:
    """
    Generate dim_date from min/max txn_timestamp in transactions.parquet.
    dim_date is immutable — regenerated fresh on every pipeline run.
    """
    txns = pd.read_parquet(parquet_dir / "transactions.parquet")
    dates = pd.to_datetime(txns["txn_timestamp"]).dropna()
    if dates.empty:
        raise ValueError("transactions.parquet contains no valid txn_timestamp values")
    date_range = pd.date_range(start=dates.min().date(), end=dates.max().date(), freq="D")

    df = pd.DataFrame({"full_date": date_range})
    df["date_sk"]     = (df["full_date"] - pd.Timestamp("2000-01-01")).dt.days.astype(int)
    df["day_of_week"] = df["full_date"].dt.dayofweek + 1          # 1=Mon … 7=Sun
    df["day_name"]    = df["full_date"].dt.day_name()
    df["week_number"] = df["full_date"].dt.isocalendar().week.astype(int)
    df["month"]       = df["full_date"].dt.month
    df["month_name"]  = df["full_date"].dt.month_name()
    df["quarter"]     = df["full_date"].dt.quarter
    df["year"]        = df["full_date"].dt.year
    df["is_weekend"]  = df["full_date"].dt.dayofweek >= 5
    df["full_date"]   = df["full_date"].dt.date

    log.info("  dim_date → %d rows", len(df))
    return df[["date_sk", "full_date", "day_of_week", "day_name",
               "week_number", "month", "month_name", "quarter", "year", "is_weekend"]]


# ══════════════════════════════════════════════════════
#  Branch Dimension Builder
# ══════════════════════════════════════════════════════

def build_dim_branch(parquet_dir: Path) -> pd.DataFrame:
    """
    Build dim_branch current-state snapshot from branches.parquet.
    branch_sk (surrogate key) and SCD2 columns are assigned by load_scd2().
    """
    branches = pd.read_parquet(parquet_dir / "branches.parquet")
    result = (
        branches[["branch_id", "branch_name", "city", "state", "country"]]
        .copy()
        .sort_values("branch_id")
        .reset_index(drop=True)
    )
    log.info("  dim_branch snapshot → %d rows", len(result))
    return result


# ══════════════════════════════════════════════════════
#  Mapping Sheet Parser
# ══════════════════════════════════════════════════════

def parse_mapping_sheet(xlsx_path: str | Path) -> dict:
    """
    Parse the DataBridge mapping sheet Excel file.

    Returns
    -------
    dict with keys:
        - "source_tables": list of source table DDLs (informational)
        - "target_tables": list of target table DDLs (informational)
        - "mappings": dict keyed by target table name → list of column rules
          Each rule: {target_col, target_dtype, source_table, source_col, transform_rule}
        - "required_source_tables": set of unique source table names needed
    """
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)

    # ── Parse Data_Mapping_sheet ──────────────────────
    ws = wb["Data_Mapping_sheet"]
    rows = list(ws.iter_rows(min_row=1, values_only=True))

    mappings: dict[str, list[dict]] = {}
    current_target = None

    for row in rows:
        # Skip completely empty rows
        if all(cell is None for cell in row):
            continue

        col_a = str(row[0]).strip() if row[0] else ""
        col_b = str(row[1]).strip() if row[1] else ""

        # Skip header rows
        if col_b.lower() in ("target column", "target columns") or col_a.lower() == "target tables":
            continue

        target_table = col_a
        if target_table and col_b:
            rule = {
                "target_col":     str(row[1]).strip() if row[1] else "",
                "target_dtype":   str(row[2]).strip() if row[2] else "",
                "source_table":   str(row[3]).strip() if row[3] else "None",
                "source_col":     str(row[4]).strip() if row[4] else "None",
                "transform_rule": str(row[5]).strip() if row[5] else "",
                "enricher":       str(row[6]).strip() if len(row) > 6 and row[6] and str(row[6]).strip() not in ("None", "") else "",
                "scd_type":       (lambda v: int(float(str(v))))(row[7]) if len(row) > 7 and row[7] is not None else 1,
            }
            if target_table not in mappings:
                mappings[target_table] = []
            mappings[target_table].append(rule)

    wb.close()

    # ── Derive required source tables ─────────────────
    required = set()
    for rules in mappings.values():
        for rule in rules:
            src = rule["source_table"]
            if src and src.lower() not in ("none", ""):
                required.add(src)

    return {
        "mappings": mappings,
        "required_source_tables": required,
    }


def get_required_source_tables(mapping: dict) -> list[str]:
    """Extract the list of source tables needed for transformation."""
    required = set(mapping.get("required_source_tables", set()))
    
    # Exclude the target tables themselves (e.g. dim_account_customer) 
    # since they are built dynamically in phase 2, not extracted from PG.
    target_tables = set(mapping.get("mappings", {}).keys())
    return sorted(required - target_tables)


# ══════════════════════════════════════════════════════
#  Star-Schema Builders
# ══════════════════════════════════════════════════════

def build_dim_account_customer(parquet_dir: Path) -> pd.DataFrame:
    """
    Build the dim_account_customer dimension table from raw Parquet files.

    Transformation logic (from mapping sheet):
      - accounts INNER JOIN customers ON customer_id
      - LEFT JOIN branches ON branch_id
      - LEFT JOIN cards ON account_id → aggregate has_active_card
      - Generate account_sk (auto-incrementing surrogate key)
      - Select & rename columns per mapping spec
    """
    log.info("Building dim_account_customer …")

    # ── Read source Parquet files ─────────────────────
    accounts = pd.read_parquet(parquet_dir / "accounts.parquet")
    customers = pd.read_parquet(parquet_dir / "customers.parquet")
    branches = pd.read_parquet(parquet_dir / "branches.parquet")
    cards = pd.read_parquet(parquet_dir / "cards.parquet")

    # ── Step 1: accounts INNER JOIN customers ─────────
    dim = accounts.merge(
        customers[["customer_id", "full_name", "state"]],
        on="customer_id",
        how="inner",
    )

    # ── Step 2: LEFT JOIN branches ────────────────────
    dim = dim.merge(
        branches[["branch_id", "branch_name"]],
        on="branch_id",
        how="left",
    )

    # ── Step 3: Derive has_active_card ────────────────
    # Group cards by account_id; TRUE if ANY card is active
    card_flag = (
        cards.groupby("account_id")["is_active"]
        .any()
        .reset_index()
        .rename(columns={"is_active": "has_active_card"})
    )
    dim = dim.merge(card_flag, on="account_id", how="left")
    dim["has_active_card"] = dim["has_active_card"].fillna(False).infer_objects(copy=False).astype(bool)

    # ── Step 4: Generate surrogate key ────────────────
    dim = dim.sort_values("account_id").reset_index(drop=True)
    dim["account_sk"] = range(1, len(dim) + 1)

    # ── Step 5: Select & rename to target schema ──────
    result = dim[
        [
            "account_sk",
            "account_id",
            "customer_id",
            "full_name",
            "state",
            "branch_name",
            "account_type",
            "has_active_card",
        ]
    ].rename(
        columns={
            "full_name": "customer_name",
            "state": "customer_state",
        }
    )

    # ── Cast types ────────────────────────────────────
    result["account_sk"] = result["account_sk"].astype(int)
    result["account_id"] = result["account_id"].astype(int)
    result["customer_id"] = result["customer_id"].astype(int)

    log.info("  dim_account_customer → %d rows, %d cols", len(result), len(result.columns))
    return result


def build_fact_transactions(
    parquet_dir: Path,
    dim_df: pd.DataFrame,
    dim_date_df: pd.DataFrame,
    dim_branch_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build fact_transactions.

    New FKs added vs original:
      - date_sk    → lookup from dim_date by transaction_date
      - branch_sk  → lookup from dim_branch via accounts.branch_id
    """
    log.info("Building fact_transactions …")

    transactions      = pd.read_parquet(parquet_dir / "transactions.parquet")
    transaction_types = pd.read_parquet(parquet_dir / "transaction_types.parquet")
    accounts          = pd.read_parquet(parquet_dir / "accounts.parquet")

    # ── Step 1: JOIN transaction_types ───────────────
    fact = transactions.merge(
        transaction_types[["type_id", "type_name"]], on="type_id", how="left"
    )

    # ── Step 2: Lookup account_sk ─────────────────────
    fact = fact.merge(dim_df[["account_sk", "account_id"]], on="account_id", how="left")

    # ── Step 3: Cast timestamp → DATE ────────────────
    fact["transaction_date"] = pd.to_datetime(fact["txn_timestamp"]).dt.date

    # ── Step 4: Lookup date_sk ────────────────────────
    date_lookup = dim_date_df[["date_sk", "full_date"]].copy()
    date_lookup["full_date"] = pd.to_datetime(date_lookup["full_date"]).dt.date
    fact = fact.merge(
        date_lookup.rename(columns={"full_date": "transaction_date"}),
        on="transaction_date",
        how="left",
    )

    # ── Step 5: Lookup branch_sk via accounts ─────────
    acct_branch = accounts[["account_id", "branch_id"]].drop_duplicates()
    fact = fact.merge(acct_branch, on="account_id", how="left")
    fact = fact.merge(
        dim_branch_df[["branch_sk", "branch_id"]], on="branch_id", how="left"
    )

    # ── Step 6: Select & cast ─────────────────────────
    result = fact[[
        "transaction_id", "account_sk", "date_sk", "branch_sk",
        "transaction_date", "type_name", "amount",
    ]].rename(columns={"type_name": "transaction_type"})

    result["transaction_id"] = result["transaction_id"].astype(int)
    result["account_sk"]     = result["account_sk"].astype("Int64")
    result["date_sk"]        = result["date_sk"].astype("Int64")
    result["branch_sk"]      = result["branch_sk"].astype("Int64")
    result["amount"]         = result["amount"].astype(float)

    log.info("  fact_transactions → %d rows, %d cols", len(result), len(result.columns))
    return result


# ══════════════════════════════════════════════════════
#  Hybrid Engine
# ══════════════════════════════════════════════════════

# Table-builder enrichers return a full pd.DataFrame.
# Column enrichers are not used in this registry — they are
# expressed as DIRECT/RENAME/CAST rules in the mapping sheet.
TABLE_BUILDER_ENRICHERS: set[str] = {
    "generate_date_dim",
    "build_dim_account_customer_enricher",
}

ENRICHER_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "generate_date_dim": lambda parquet_dir, ctx: build_dim_date(parquet_dir),
    "build_dim_account_customer_enricher": lambda parquet_dir, ctx: build_dim_account_customer(parquet_dir),
}

assert TABLE_BUILDER_ENRICHERS <= set(ENRICHER_REGISTRY), (
    f"TABLE_BUILDER_ENRICHERS contains names not in ENRICHER_REGISTRY: "
    f"{TABLE_BUILDER_ENRICHERS - set(ENRICHER_REGISTRY)}"
)

# Build order: dims before fact.
# fact_transactions is built separately in pipeline.py after dims are loaded.
_BUILD_ORDER = ["dim_date", "dim_branch", "dim_account_customer"]


def _apply_generic_rules(
    rules: list[dict],
    parquet_dir: Path,
) -> pd.DataFrame:
    """
    Apply DIRECT, RENAME, and CAST:* rules to produce a DataFrame.

    Reads each unique source_table once, then maps columns.
    Assumes all rules for a given target table share the same primary source_table
    (i.e. single-source tables like dim_branch). Multi-table joins must use enrichers.
    """
    # Collect unique source tables (ignore placeholder '—' and 'None')
    source_tables = {
        r["source_table"] for r in rules
        if r["source_table"] not in ("—", "None", "", "none")
    }

    if not source_tables:
        return pd.DataFrame()

    # Read and merge source tables (for single-source tables this is just one read)
    source_dfs: dict[str, pd.DataFrame] = {
        t: pd.read_parquet(parquet_dir / f"{t}.parquet") for t in source_tables
    }

    result_cols: dict[str, pd.Series] = {}

    for rule in rules:
        if rule["enricher"]:
            continue  # handled in enricher pass

        src_table = rule["source_table"]
        src_col   = rule["source_col"]
        tgt_col   = rule["target_col"]
        transform = rule["transform_rule"].upper()
        src_df    = source_dfs.get(src_table, next(iter(source_dfs.values())))

        if src_col not in ("None", "—", "") and src_col in src_df.columns:
            if transform in ("DIRECT", "RENAME"):
                result_cols[tgt_col] = src_df[src_col].values
            elif transform.startswith("CAST:DATE"):
                result_cols[tgt_col] = pd.to_datetime(src_df[src_col]).dt.date
            elif transform.startswith("CAST:INT"):
                result_cols[tgt_col] = src_df[src_col].astype(int)
            elif transform.startswith("CAST:BOOL"):
                result_cols[tgt_col] = src_df[src_col].astype(bool)

    return pd.DataFrame(result_cols)


def run_hybrid_transforms(
    parquet_dir: Path,
    mapping: dict,
    progress_callback=None,
) -> dict[str, pd.DataFrame]:
    """
    Orchestrate dim-table transforms using the hybrid engine.

    For each target table:
      - If ANY rule has an enricher in TABLE_BUILDER_ENRICHERS → call that enricher.
      - Otherwise → apply DIRECT/RENAME/CAST rules generically.

    Returns DataFrames for dim tables only. fact_transactions is built
    separately in pipeline.py after dims are loaded into DuckDB.
    """
    results: dict[str, pd.DataFrame] = {}
    total = len(_BUILD_ORDER)

    for step, target_table in enumerate(_BUILD_ORDER, start=1):
        if target_table not in mapping.get("mappings", {}):
            continue

        rules = mapping["mappings"][target_table]

        if progress_callback:
            progress_callback("Transform", f"Building {target_table}", step / total)

        # Check for a table-builder enricher
        builder_name = next(
            (r["enricher"] for r in rules if r["enricher"] in TABLE_BUILDER_ENRICHERS),
            None,
        )

        # Check for any unknown enricher (raise early, clear error message)
        for rule in rules:
            name = rule["enricher"]
            if name and name not in ENRICHER_REGISTRY:
                raise KeyError(
                    f"Enricher '{name}' not found in ENRICHER_REGISTRY. "
                    f"Available: {sorted(ENRICHER_REGISTRY.keys())}"
                )

        if builder_name:
            df = ENRICHER_REGISTRY[builder_name](parquet_dir, results)
        else:
            df = _apply_generic_rules(rules, parquet_dir)

        results[target_table] = df
        log.info("  run_hybrid_transforms: built %s (%d rows)", target_table, len(df))

    return results


def run_mapping_transforms(
    parquet_dir: Path,
    mapping: dict | None = None,
    progress_callback=None,
) -> dict[str, pd.DataFrame]:
    """
    Orchestrate all mapping-sheet transformations.

    Parameters
    ----------
    parquet_dir : Path
        Directory containing the raw Parquet files.
    mapping : dict | None
        Parsed mapping sheet (for future generic use).
    progress_callback : callable | None
        Optional callback(phase_name, step, total) for progress.

    Returns
    -------
    dict[str, pd.DataFrame]
        {"dim_account_customer": df, "fact_transactions": df}
    """
    if progress_callback:
        progress_callback("Building dimension table", 1, 3)

    dim_df = build_dim_account_customer(parquet_dir)

    if progress_callback:
        progress_callback("Building fact table", 2, 3)

    fact_df = build_fact_transactions(parquet_dir, dim_df)

    if progress_callback:
        progress_callback("Transforms complete", 3, 3)

    return {
        "dim_account_customer": dim_df,
        "fact_transactions": fact_df,
    }


# ══════════════════════════════════════════════════════
#  Generic Cleaning Utilities (preserved from original)
# ══════════════════════════════════════════════════════

def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact-duplicate rows."""
    return df.drop_duplicates().reset_index(drop=True)


def drop_null_rows(df: pd.DataFrame, subset: list[str] | None = None) -> pd.DataFrame:
    """Drop rows where *all* (or specified) columns are null."""
    return df.dropna(subset=subset).reset_index(drop=True)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and snake_case all column names."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )
    return df


def mask_column(df: pd.DataFrame, col: str, keep_last: int = 4) -> pd.DataFrame:
    """Replace characters with '*' except the last `keep_last`."""
    df = df.copy()
    df[col] = df[col].astype(str).apply(
        lambda v: "*" * max(0, len(v) - keep_last) + v[-keep_last:]
    )
    return df


def aggregate(
    df: pd.DataFrame,
    group_cols: list[str],
    agg_map: dict[str, str],
) -> pd.DataFrame:
    """Group by `group_cols` and apply `agg_map` (col → func)."""
    return df.groupby(group_cols, as_index=False).agg(agg_map)


def run_default_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the standard cleaning pipeline to any DataFrame."""
    df = normalize_columns(df)
    df = drop_duplicates(df)
    df = drop_null_rows(df)
    return df
