"""Tests for load_scd2() — SCD Type 2 upsert strategy."""
import pytest
import pandas as pd
from datetime import date, timedelta
from src.load import get_duck_conn, load_scd2

TODAY     = date(2024, 6, 1)
TOMORROW  = TODAY + timedelta(days=1)
YESTERDAY = TODAY - timedelta(days=1)


@pytest.fixture
def conn():
    c = get_duck_conn(":memory:")
    yield c
    c.close()


@pytest.fixture
def branch_df():
    return pd.DataFrame({
        "branch_id":   [1, 2],
        "branch_name": ["North", "South"],
        "city":        ["Chennai", "Mumbai"],
        "state":       ["TN", "MH"],
        "country":     ["India", "India"],
    })


def _scd2(conn, df, today=TODAY, sk=None):
    load_scd2(
        conn, df, "dim_branch", "branch_id",
        ["branch_name", "city", "state"],
        surrogate_key_col=sk,
        today=today,
    )


def test_initial_load_row_count(conn, branch_df):
    _scd2(conn, branch_df)
    assert conn.execute("SELECT COUNT(*) FROM dim_branch").fetchone()[0] == 2


def test_initial_load_scd2_cols_present(conn, branch_df):
    _scd2(conn, branch_df)
    cols = conn.execute("DESCRIBE dim_branch").fetchdf()["column_name"].tolist()
    for col in ("effective_from", "effective_to", "is_current", "version"):
        assert col in cols, f"Missing column: {col}"


def test_initial_load_all_current(conn, branch_df):
    _scd2(conn, branch_df)
    result = conn.execute("SELECT * FROM dim_branch WHERE is_current = TRUE").fetchdf()
    assert len(result) == 2


def test_initial_load_version_1(conn, branch_df):
    _scd2(conn, branch_df)
    assert conn.execute("SELECT MIN(version) FROM dim_branch").fetchone()[0] == 1
    assert conn.execute("SELECT MAX(version) FROM dim_branch").fetchone()[0] == 1


def test_initial_load_effective_to_null(conn, branch_df):
    _scd2(conn, branch_df)
    result = conn.execute("SELECT * FROM dim_branch").fetchdf()
    assert result["effective_to"].isna().all()


def test_unchanged_rows_not_duplicated(conn, branch_df):
    _scd2(conn, branch_df, today=TODAY)
    _scd2(conn, branch_df, today=TOMORROW)
    assert conn.execute("SELECT COUNT(*) FROM dim_branch").fetchone()[0] == 2


def test_changed_tracked_col_creates_new_version(conn, branch_df):
    _scd2(conn, branch_df, today=TODAY)

    updated = branch_df.copy()
    updated.loc[updated["branch_id"] == 1, "branch_name"] = "North Updated"
    _scd2(conn, updated, today=TOMORROW)

    # 3 rows: branch_id=1 v1 (expired), branch_id=1 v2 (current), branch_id=2 v1
    assert conn.execute("SELECT COUNT(*) FROM dim_branch").fetchone()[0] == 3


def test_old_version_is_expired(conn, branch_df):
    _scd2(conn, branch_df, today=TODAY)
    updated = branch_df.copy()
    updated.loc[updated["branch_id"] == 1, "branch_name"] = "North Updated"
    _scd2(conn, updated, today=TOMORROW)

    old = conn.execute(
        "SELECT * FROM dim_branch WHERE branch_id = 1 AND version = 1"
    ).fetchdf().iloc[0]
    assert old["is_current"] == False
    assert pd.to_datetime(old["effective_to"]).date() == TODAY


def test_new_version_is_current(conn, branch_df):
    _scd2(conn, branch_df, today=TODAY)
    updated = branch_df.copy()
    updated.loc[updated["branch_id"] == 1, "branch_name"] = "North Updated"
    _scd2(conn, updated, today=TOMORROW)

    new = conn.execute(
        "SELECT * FROM dim_branch WHERE branch_id = 1 AND is_current = TRUE"
    ).fetchdf().iloc[0]
    assert new["version"] == 2
    assert new["branch_name"] == "North Updated"


def test_new_natural_key_inserted_as_v1(conn, branch_df):
    _scd2(conn, branch_df, today=TODAY)

    with_new = pd.concat([
        branch_df,
        pd.DataFrame({"branch_id": [3], "branch_name": ["West"],
                      "city": ["Kolkata"], "state": ["WB"], "country": ["India"]}),
    ], ignore_index=True)
    _scd2(conn, with_new, today=TOMORROW)

    new_row = conn.execute(
        "SELECT * FROM dim_branch WHERE branch_id = 3"
    ).fetchdf().iloc[0]
    assert new_row["version"] == 1
    assert new_row["is_current"] == True


def test_surrogate_key_auto_assigned_on_initial(conn, branch_df):
    """When surrogate_key_col is set and not in df, load_scd2 assigns 1, 2, ..."""
    _scd2(conn, branch_df, sk="branch_sk")
    result = conn.execute(
        "SELECT branch_sk FROM dim_branch ORDER BY branch_sk"
    ).fetchdf()
    assert list(result["branch_sk"]) == [1, 2]


def test_surrogate_key_incremented_for_new_version(conn, branch_df):
    _scd2(conn, branch_df, sk="branch_sk")
    updated = branch_df.copy()
    updated.loc[updated["branch_id"] == 1, "branch_name"] = "North Updated"
    _scd2(conn, updated, today=TOMORROW, sk="branch_sk")

    new_sk = conn.execute(
        "SELECT branch_sk FROM dim_branch WHERE branch_id = 1 AND is_current = TRUE"
    ).fetchone()[0]
    # Must be exactly 3 (max from initial load is 2, next is 3)
    assert new_sk == 3


def test_type1_col_updated_in_place(conn, branch_df):
    """Non-tracked columns (Type 1) are updated in-place without creating a new version."""
    _scd2(conn, branch_df, today=TODAY)

    updated = branch_df.copy()
    updated.loc[updated["branch_id"] == 1, "country"] = "IN"
    _scd2(conn, updated, today=TOMORROW)

    # Still 2 rows — no new version created
    assert conn.execute("SELECT COUNT(*) FROM dim_branch").fetchone()[0] == 2
    # Country updated in-place on current row
    row = conn.execute(
        "SELECT country FROM dim_branch WHERE branch_id = 1 AND is_current = TRUE"
    ).fetchdf().iloc[0]
    assert row["country"] == "IN"


def test_multi_step_history(conn, branch_df):
    """Two successive changes to same key produce three version rows."""
    _scd2(conn, branch_df, today=TODAY)

    step2 = branch_df.copy()
    step2.loc[step2["branch_id"] == 1, "branch_name"] = "North v2"
    _scd2(conn, step2, today=TOMORROW)

    step3 = step2.copy()
    step3.loc[step3["branch_id"] == 1, "branch_name"] = "North v3"
    _scd2(conn, step3, today=TOMORROW + timedelta(days=1))

    # 4 rows: id=1 v1, id=1 v2, id=1 v3, id=2 v1
    assert conn.execute("SELECT COUNT(*) FROM dim_branch").fetchone()[0] == 4
    # Only v3 is current
    curr = conn.execute(
        "SELECT version FROM dim_branch WHERE branch_id = 1 AND is_current = TRUE"
    ).fetchone()[0]
    assert curr == 3
