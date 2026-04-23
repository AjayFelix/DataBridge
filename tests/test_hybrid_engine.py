"""Tests for the hybrid mapping engine."""
import pytest
import pandas as pd
from pathlib import Path
from src.transform import run_hybrid_transforms


@pytest.fixture
def branch_parquet(tmp_path):
    pd.DataFrame({
        "branch_id":   [1, 2],
        "branch_name": ["North", "South"],
        "city":        ["Chennai", "Mumbai"],
        "state":       ["TN", "MH"],
        "country":     ["India", "India"],
    }).to_parquet(tmp_path / "branches.parquet", index=False)
    pd.DataFrame({
        "transaction_id": [1],
        "account_id":     [1],
        "type_id":        [1],
        "amount":         [100.0],
        "txn_timestamp":  pd.to_datetime(["2024-01-15"]),
        "status":         ["completed"],
        "reference_id":   ["R1"],
    }).to_parquet(tmp_path / "transactions.parquet", index=False)
    return tmp_path


def _make_mapping(rules_by_table):
    required = set()
    for rules in rules_by_table.values():
        for r in rules:
            if r.get("source_table") not in (None, "", "None", "—"):
                required.add(r["source_table"])
    return {"mappings": rules_by_table, "required_source_tables": required}


def test_direct_rule_copies_column(branch_parquet):
    mapping = _make_mapping({
        "dim_branch": [
            {"target_col": "branch_id",   "source_table": "branches", "source_col": "branch_id",
             "transform_rule": "DIRECT", "enricher": "", "scd_type": 2},
            {"target_col": "branch_name", "source_table": "branches", "source_col": "branch_name",
             "transform_rule": "DIRECT", "enricher": "", "scd_type": 2},
        ]
    })
    results = run_hybrid_transforms(branch_parquet, mapping)
    assert "dim_branch" in results
    assert "branch_id" in results["dim_branch"].columns
    assert list(results["dim_branch"]["branch_id"]) == [1, 2]


def test_rename_rule_renames_column(branch_parquet):
    mapping = _make_mapping({
        "dim_branch": [
            {"target_col": "city_name", "source_table": "branches", "source_col": "city",
             "transform_rule": "RENAME", "enricher": "", "scd_type": 2},
        ]
    })
    results = run_hybrid_transforms(branch_parquet, mapping)
    assert "city_name" in results["dim_branch"].columns
    assert "city" not in results["dim_branch"].columns


def test_generate_date_dim_enricher(branch_parquet):
    mapping = _make_mapping({
        "dim_date": [
            {"target_col": "dim_date_placeholder", "source_table": "—", "source_col": "—",
             "transform_rule": "DERIVED", "enricher": "generate_date_dim", "scd_type": 1},
        ]
    })
    results = run_hybrid_transforms(branch_parquet, mapping)
    assert "dim_date" in results
    assert "date_sk" in results["dim_date"].columns


def test_unknown_enricher_raises_key_error(branch_parquet):
    mapping = _make_mapping({
        "dim_branch": [
            {"target_col": "x", "source_table": "—", "source_col": "—",
             "transform_rule": "DERIVED", "enricher": "nonexistent_enricher", "scd_type": 1},
        ]
    })
    with pytest.raises(KeyError, match="nonexistent_enricher"):
        run_hybrid_transforms(branch_parquet, mapping)


def test_table_builder_enricher_returns_full_df(branch_parquet):
    mapping = _make_mapping({
        "dim_date": [
            {"target_col": "_", "source_table": "—", "source_col": "—",
             "transform_rule": "DERIVED", "enricher": "generate_date_dim", "scd_type": 1},
        ]
    })
    results = run_hybrid_transforms(branch_parquet, mapping)
    assert len(results["dim_date"]) > 0
    assert "is_weekend" in results["dim_date"].columns
