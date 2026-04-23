"""
End-to-end integration test for the galaxy schema pipeline.
Uses synthetic Parquet files in tmp_path — no PostgreSQL required.
"""
import pytest
import pandas as pd
from pathlib import Path
from src.transform import (
    build_dim_date, build_dim_branch, build_dim_account_customer,
    build_fact_transactions,
)
from src.load import get_duck_conn, load_table, load_scd2
from src.config import (
    DIM_TABLE, FACT_TABLE, DIM_DATE_TABLE, DIM_BRANCH_TABLE,
    SCD2_TRACKED_COLS, SCD2_NATURAL_KEYS, SCD2_SURROGATE_KEYS,
)


@pytest.fixture
def staging(tmp_path):
    """Create a complete synthetic Parquet staging directory."""
    pd.DataFrame({
        "branch_id":   [1, 2],
        "branch_name": ["North", "South"],
        "city":        ["Chennai", "Mumbai"],
        "state":       ["TN", "MH"],
        "country":     ["India", "India"],
    }).to_parquet(tmp_path / "branches.parquet", index=False)

    pd.DataFrame({
        "customer_id": [1, 2],
        "full_name":   ["Alice", "Bob"],
        "email":       ["a@x.com", "b@x.com"],
        "phone":       ["111", "222"],
        "state":       ["TN", "MH"],
        "onboarding_date": pd.to_datetime(["2020-01-01", "2021-06-01"]),
        "kyc_status":  ["verified", "verified"],
    }).to_parquet(tmp_path / "customers.parquet", index=False)

    pd.DataFrame({
        "account_id":   [10, 20],
        "customer_id":  [1, 2],
        "branch_id":    [1, 2],
        "account_type": ["Savings", "Current"],
        "balance":      [5000.0, 12000.0],
        "currency":     ["INR", "INR"],
        "status":       ["active", "active"],
        "opened_date":  pd.to_datetime(["2021-01-01", "2022-03-01"]),
    }).to_parquet(tmp_path / "accounts.parquet", index=False)

    pd.DataFrame({
        "card_id":    [1, 2],
        "account_id": [10, 20],
        "card_number": ["4111", "5500"],
        "card_type":  ["Visa", "MC"],
        "expiry_date": pd.to_datetime(["2027-01-01", "2026-06-01"]),
        "is_active":  [True, False],
    }).to_parquet(tmp_path / "cards.parquet", index=False)

    pd.DataFrame({
        "type_id":     [1, 2],
        "type_name":   ["Deposit", "Withdrawal"],
        "description": ["Credit", "Debit"],
    }).to_parquet(tmp_path / "transaction_types.parquet", index=False)

    pd.DataFrame({
        "transaction_id": [1, 2, 3],
        "account_id":     [10, 20, 10],
        "type_id":        [1, 2, 1],
        "amount":         [500.0, 200.0, 300.0],
        "txn_timestamp":  pd.to_datetime(["2024-01-15", "2024-03-20", "2024-06-10"]),
        "status":         ["completed", "completed", "completed"],
        "reference_id":   ["R1", "R2", "R3"],
    }).to_parquet(tmp_path / "transactions.parquet", index=False)

    return tmp_path


def _run_pipeline(staging):
    """Run the pipeline phases against an in-memory DuckDB."""
    conn = get_duck_conn(":memory:")

    dim_date_df   = build_dim_date(staging)
    dim_branch_df = build_dim_branch(staging)
    dim_df        = build_dim_account_customer(staging)

    load_table(conn, dim_date_df, DIM_DATE_TABLE, mode="replace")
    load_scd2(conn, dim_branch_df, DIM_BRANCH_TABLE,
              natural_key=SCD2_NATURAL_KEYS[DIM_BRANCH_TABLE],
              tracked_cols=SCD2_TRACKED_COLS[DIM_BRANCH_TABLE],
              surrogate_key_col=SCD2_SURROGATE_KEYS[DIM_BRANCH_TABLE])
    load_scd2(conn, dim_df, DIM_TABLE,
              natural_key=SCD2_NATURAL_KEYS[DIM_TABLE],
              tracked_cols=SCD2_TRACKED_COLS[DIM_TABLE],
              surrogate_key_col=SCD2_SURROGATE_KEYS[DIM_TABLE])

    dim_lookup    = conn.execute(f"SELECT account_sk, account_id FROM {DIM_TABLE} WHERE is_current = TRUE").fetchdf()
    branch_lookup = conn.execute(f"SELECT branch_sk, branch_id FROM {DIM_BRANCH_TABLE} WHERE is_current = TRUE").fetchdf()

    fact_df = build_fact_transactions(staging, dim_lookup, dim_date_df, branch_lookup)
    load_table(conn, fact_df, FACT_TABLE, mode="replace")

    return conn, dim_date_df, dim_df, fact_df


def test_all_four_tables_exist(staging):
    conn, *_ = _run_pipeline(staging)
    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    for t in (DIM_DATE_TABLE, DIM_BRANCH_TABLE, DIM_TABLE, FACT_TABLE):
        assert t in tables, f"Missing table: {t}"
    conn.close()


def test_dim_tables_nonzero(staging):
    conn, dim_date_df, dim_df, fact_df = _run_pipeline(staging)
    assert len(dim_date_df) > 0
    assert conn.execute(f"SELECT COUNT(*) FROM {DIM_BRANCH_TABLE}").fetchone()[0] > 0
    assert conn.execute(f"SELECT COUNT(*) FROM {DIM_TABLE}").fetchone()[0] > 0
    conn.close()


def test_fact_date_sk_fk_integrity(staging):
    """Every date_sk in fact_transactions must exist in dim_date."""
    conn, *_ = _run_pipeline(staging)
    orphans = conn.execute(f"""
        SELECT COUNT(*) FROM {FACT_TABLE} f
        WHERE f.date_sk NOT IN (SELECT date_sk FROM {DIM_DATE_TABLE})
    """).fetchone()[0]
    assert orphans == 0, f"{orphans} orphaned date_sk values in fact_transactions"
    conn.close()


def test_fact_branch_sk_fk_integrity(staging):
    """Every branch_sk in fact_transactions must exist in dim_branch."""
    conn, *_ = _run_pipeline(staging)
    orphans = conn.execute(f"""
        SELECT COUNT(*) FROM {FACT_TABLE} f
        WHERE f.branch_sk IS NOT NULL
          AND f.branch_sk NOT IN (SELECT branch_sk FROM {DIM_BRANCH_TABLE})
    """).fetchone()[0]
    assert orphans == 0, f"{orphans} orphaned branch_sk values"
    conn.close()


def test_dim_branch_has_scd2_cols(staging):
    conn, *_ = _run_pipeline(staging)
    cols = conn.execute(f"DESCRIBE {DIM_BRANCH_TABLE}").fetchdf()["column_name"].tolist()
    for col in ("effective_from", "effective_to", "is_current", "version", "branch_sk"):
        assert col in cols
    conn.close()


def test_fact_row_count(staging):
    conn, _, _, fact_df = _run_pipeline(staging)
    assert len(fact_df) == 3
    conn.close()
