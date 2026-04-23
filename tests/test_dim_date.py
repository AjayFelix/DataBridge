"""Tests for build_dim_date()."""
import pytest
import pandas as pd
from src.transform import build_dim_date


@pytest.fixture
def parquet_dir(tmp_path):
    pd.DataFrame({
        "transaction_id": [1, 2, 3],
        "account_id":     [1, 2, 3],
        "type_id":        [1, 1, 2],
        "amount":         [100.0, 200.0, 300.0],
        "txn_timestamp":  pd.to_datetime(["2024-01-01", "2024-03-15", "2024-06-30"]),
        "status":         ["completed", "completed", "pending"],
        "reference_id":   ["R1", "R2", "R3"],
    }).to_parquet(tmp_path / "transactions.parquet", index=False)
    return tmp_path


def test_date_range_start(parquet_dir):
    df = build_dim_date(parquet_dir)
    assert pd.to_datetime(df["full_date"]).min().date() == pd.Timestamp("2024-01-01").date()


def test_date_range_end(parquet_dir):
    df = build_dim_date(parquet_dir)
    assert pd.to_datetime(df["full_date"]).max().date() == pd.Timestamp("2024-06-30").date()


def test_no_duplicate_date_sk(parquet_dir):
    df = build_dim_date(parquet_dir)
    assert df["date_sk"].nunique() == len(df)


def test_saturday_is_weekend(parquet_dir):
    df = build_dim_date(parquet_dir)
    # 2024-01-06 is a Saturday
    row = df[df["full_date"] == pd.Timestamp("2024-01-06").date()].iloc[0]
    assert row["is_weekend"] == True


def test_monday_not_weekend(parquet_dir):
    df = build_dim_date(parquet_dir)
    # 2024-01-08 is a Monday
    row = df[df["full_date"] == pd.Timestamp("2024-01-08").date()].iloc[0]
    assert row["is_weekend"] == False


def test_january_is_q1(parquet_dir):
    df = build_dim_date(parquet_dir)
    assert df[df["month"] == 1].iloc[0]["quarter"] == 1


def test_april_is_q2(parquet_dir):
    df = build_dim_date(parquet_dir)
    assert df[df["month"] == 4].iloc[0]["quarter"] == 2


def test_required_columns(parquet_dir):
    df = build_dim_date(parquet_dir)
    required = {"date_sk", "full_date", "day_of_week", "day_name",
                "week_number", "month", "month_name", "quarter", "year", "is_weekend"}
    assert required.issubset(set(df.columns))
