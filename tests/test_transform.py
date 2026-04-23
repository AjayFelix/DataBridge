"""Tests for M2 – Transform module (no external deps needed)."""

import pytest
import pandas as pd
from pathlib import Path
from src.transform import (
    drop_duplicates,
    drop_null_rows,
    normalize_columns,
    mask_column,
    aggregate,
    run_default_transforms,
    build_fact_transactions,
    build_dim_date,
)


def test_drop_duplicates():
    df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 3, 4]})
    result = drop_duplicates(df)
    assert len(result) == 2


def test_drop_null_rows():
    df = pd.DataFrame({"a": [1, None, 3], "b": [4, None, 6]})
    result = drop_null_rows(df)
    assert len(result) == 2


def test_normalize_columns():
    df = pd.DataFrame({"First Name": [1], "Last   Name": [2]})
    result = normalize_columns(df)
    assert list(result.columns) == ["first_name", "last_name"]


def test_mask_column():
    df = pd.DataFrame({"ssn": ["123456789"]})
    result = mask_column(df, "ssn", keep_last=4)
    assert result["ssn"].iloc[0] == "*****6789"


def test_aggregate():
    df = pd.DataFrame({"dept": ["A", "A", "B"], "salary": [10, 20, 30]})
    result = aggregate(df, ["dept"], {"salary": "sum"})
    assert result.loc[result["dept"] == "A", "salary"].iloc[0] == 30


def test_run_default_transforms():
    df = pd.DataFrame({"Col A": [1, 1, None], "Col B": [2, 2, None]})
    result = run_default_transforms(df)
    assert list(result.columns) == ["col_a", "col_b"]
    assert len(result) == 1


@pytest.fixture
def star_parquet(tmp_path):
    """Synthetic parquet directory for fact builder tests."""
    pd.DataFrame({
        "transaction_id": [1, 2],
        "account_id":     [10, 20],
        "type_id":        [1, 2],
        "amount":         [100.0, 200.0],
        "txn_timestamp":  pd.to_datetime(["2024-03-01", "2024-06-15"]),
        "status":         ["completed", "completed"],
        "reference_id":   ["R1", "R2"],
    }).to_parquet(tmp_path / "transactions.parquet", index=False)

    pd.DataFrame({
        "type_id":   [1, 2],
        "type_name": ["Deposit", "Withdrawal"],
    }).to_parquet(tmp_path / "transaction_types.parquet", index=False)

    pd.DataFrame({
        "account_id": [10, 20],
        "branch_id":  [1, 2],
    }).to_parquet(tmp_path / "accounts.parquet", index=False)

    return tmp_path


def test_fact_has_date_sk(star_parquet):
    dim_df        = pd.DataFrame({"account_sk": [1, 2], "account_id": [10, 20]})
    dim_date_df   = build_dim_date(star_parquet)
    dim_branch_df = pd.DataFrame({"branch_sk": [1, 2], "branch_id": [1, 2]})

    fact = build_fact_transactions(star_parquet, dim_df, dim_date_df, dim_branch_df)

    assert "date_sk" in fact.columns
    assert fact["date_sk"].notna().all()


def test_fact_has_branch_sk(star_parquet):
    dim_df        = pd.DataFrame({"account_sk": [1, 2], "account_id": [10, 20]})
    dim_date_df   = build_dim_date(star_parquet)
    dim_branch_df = pd.DataFrame({"branch_sk": [1, 2], "branch_id": [1, 2]})

    fact = build_fact_transactions(star_parquet, dim_df, dim_date_df, dim_branch_df)

    assert "branch_sk" in fact.columns
    assert fact["branch_sk"].notna().all()


def test_fact_retains_core_cols(star_parquet):
    dim_df        = pd.DataFrame({"account_sk": [1, 2], "account_id": [10, 20]})
    dim_date_df   = build_dim_date(star_parquet)
    dim_branch_df = pd.DataFrame({"branch_sk": [1, 2], "branch_id": [1, 2]})

    fact = build_fact_transactions(star_parquet, dim_df, dim_date_df, dim_branch_df)

    for col in ("transaction_id", "account_sk", "transaction_type", "amount"):
        assert col in fact.columns


def test_date_sk_correct_value(star_parquet):
    """date_sk value matches days-since-2000-01-01 for each transaction date."""
    dim_df        = pd.DataFrame({"account_sk": [1, 2], "account_id": [10, 20]})
    dim_date_df   = build_dim_date(star_parquet)
    dim_branch_df = pd.DataFrame({"branch_sk": [1, 2], "branch_id": [1, 2]})

    fact = build_fact_transactions(star_parquet, dim_df, dim_date_df, dim_branch_df)

    # transaction_id=1 is 2024-03-01; days since 2000-01-01
    expected_date_sk = (pd.Timestamp("2024-03-01") - pd.Timestamp("2000-01-01")).days
    row = fact[fact["transaction_id"] == 1].iloc[0]
    assert int(row["date_sk"]) == expected_date_sk


def test_branch_sk_correct_value(star_parquet):
    """branch_sk matches the dim_branch_df entry for the account's branch_id."""
    dim_df        = pd.DataFrame({"account_sk": [1, 2], "account_id": [10, 20]})
    dim_date_df   = build_dim_date(star_parquet)
    dim_branch_df = pd.DataFrame({"branch_sk": [1, 2], "branch_id": [1, 2]})

    fact = build_fact_transactions(star_parquet, dim_df, dim_date_df, dim_branch_df)

    # account_id=10 → branch_id=1 → branch_sk=1
    row = fact[fact["transaction_id"] == 1].iloc[0]
    assert int(row["branch_sk"]) == 1

    # account_id=20 → branch_id=2 → branch_sk=2
    row2 = fact[fact["transaction_id"] == 2].iloc[0]
    assert int(row2["branch_sk"]) == 2


def test_unmatched_date_produces_null_date_sk(star_parquet):
    """Transactions whose date isn't in dim_date get null date_sk (not an error)."""
    dim_df        = pd.DataFrame({"account_sk": [1, 2], "account_id": [10, 20]})
    dim_branch_df = pd.DataFrame({"branch_sk": [1, 2], "branch_id": [1, 2]})

    # dim_date with ONLY 2024-01-01 — won't cover 2024-03-01 or 2024-06-15
    dim_date_df = pd.DataFrame({
        "date_sk":     [8766],
        "full_date":   [pd.Timestamp("2024-01-01").date()],
        "day_of_week": [1], "day_name": ["Monday"], "week_number": [1],
        "month": [1], "month_name": ["January"], "quarter": [1],
        "year": [2024], "is_weekend": [False],
    })

    fact = build_fact_transactions(star_parquet, dim_df, dim_date_df, dim_branch_df)

    # Both transactions have dates not in this truncated dim_date → null date_sk
    assert fact["date_sk"].isna().all()
    # But rows still exist (no data loss from left join)
    assert len(fact) == 2
