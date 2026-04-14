"""Tests for M2 – Transform module (no external deps needed)."""

import pandas as pd
from src.transform import (
    drop_duplicates,
    drop_null_rows,
    normalize_columns,
    mask_column,
    aggregate,
    run_default_transforms,
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
