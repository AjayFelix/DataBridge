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
from pathlib import Path

import pandas as pd
import openpyxl

log = logging.getLogger("databridge.transform")


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
                "target_col": col_b,
                "target_dtype": str(row[2]).strip() if row[2] else "",
                "source_table": str(row[3]).strip() if row[3] else "None",
                "source_col": str(row[4]).strip() if row[4] else "None",
                "transform_rule": str(row[5]).strip() if row[5] else "",
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


def build_fact_transactions(parquet_dir: Path, dim_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the fact_transactions fact table from raw Parquet files.

    Transformation logic (from mapping sheet):
      - transactions LEFT JOIN transaction_types ON type_id
      - Lookup account_sk from dim_account_customer by account_id
      - Cast txn_timestamp → transaction_date (date only)
      - Select & rename columns per mapping spec
    """
    log.info("Building fact_transactions …")

    # ── Read source Parquet files ─────────────────────
    transactions = pd.read_parquet(parquet_dir / "transactions.parquet")
    transaction_types = pd.read_parquet(parquet_dir / "transaction_types.parquet")

    # ── Step 1: LEFT JOIN transaction_types ───────────
    fact = transactions.merge(
        transaction_types[["type_id", "type_name"]],
        on="type_id",
        how="left",
    )

    # ── Step 2: Lookup account_sk from dimension ──────
    sk_lookup = dim_df[["account_sk", "account_id"]].copy()
    fact = fact.merge(sk_lookup, on="account_id", how="left")

    # ── Step 3: Cast txn_timestamp → DATE ─────────────
    fact["transaction_date"] = pd.to_datetime(fact["txn_timestamp"]).dt.date

    # ── Step 4: Select & rename to target schema ──────
    result = fact[
        [
            "transaction_id",
            "account_sk",
            "transaction_date",
            "type_name",
            "amount",
        ]
    ].rename(
        columns={
            "type_name": "transaction_type",
        }
    )

    # ── Cast types ────────────────────────────────────
    result["transaction_id"] = result["transaction_id"].astype(int)
    result["account_sk"] = result["account_sk"].astype("Int64")  # nullable for unmatched
    result["amount"] = result["amount"].astype(float)

    log.info("  fact_transactions → %d rows, %d cols", len(result), len(result.columns))
    return result


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
