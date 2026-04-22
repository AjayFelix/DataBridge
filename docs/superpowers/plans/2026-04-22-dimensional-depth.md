# DataBridge Dimensional Depth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade DataBridge from a 1-dim + 1-fact star schema to a 3-dim + 1-fact galaxy schema with SCD Type 2 history, a hybrid mapping-sheet-driven transform engine, and advanced SQL Workbench templates.

**Architecture:** A hybrid engine in `transform.py` dispatches per-table: "table builder" enrichers (returning full DataFrames) handle complex joins, while a generic two-pass mapper handles DIRECT/RENAME/CAST column rules. `load_scd2()` in `load.py` owns surrogate key assignment and SCD2 history. The pipeline orchestration interleaves dim loading and fact building so the fact table always uses DuckDB-queried current-state SKs.

**Tech Stack:** Python 3.11+, pandas 2.x, DuckDB, PyArrow, openpyxl, pytest, Streamlit, Plotly

**Spec:** `docs/superpowers/specs/2026-04-22-dimensional-depth-design.md`

---

## File Map

| File | Change |
|---|---|
| `src/config.py` | Add SCD2 constants, `DIM_DATE_TABLE`, `DIM_BRANCH_TABLE` |
| `src/transform.py` | Add `build_dim_date()`, `build_dim_branch()`, `ENRICHER_REGISTRY`, `run_hybrid_transforms()`, update `build_fact_transactions()`, update `parse_mapping_sheet()` |
| `src/load.py` | Add `load_scd2()`, update `load_star_schema()` |
| `src/pipeline.py` | Interleave dim load + fact build in `run_mapping_pipeline()` |
| `src/app.py` | Add 8 SQL templates, replace static lineage with live mapping-sheet parse |
| `DataBridge mapping sheet.xlsx` | Add `enricher` + `scd_type` columns; add rows for `dim_date`, `dim_branch`; add SCD2 + FK rows to existing tables |
| `tests/test_dim_date.py` | New — `build_dim_date()` + `build_dim_branch()` |
| `tests/test_scd2.py` | New — `load_scd2()` |
| `tests/test_hybrid_engine.py` | New — hybrid engine + enricher dispatch |
| `tests/test_pipeline_integration.py` | New — end-to-end smoke test |

---

## Task 1: Expand `config.py` with SCD2 constants

**Files:**
- Modify: `src/config.py`

- [ ] **Step 1: Add the new constants**

Open `src/config.py` and add the following block after the existing `PK_MAP` definition:

```python
# ── New Dimension Tables ──────────────────────────────
DIM_DATE_TABLE   = "dim_date"
DIM_BRANCH_TABLE = "dim_branch"

# ── SCD Type 2 Configuration ──────────────────────────
SCD2_TABLES: set[str] = {"dim_branch", "dim_account_customer"}

SCD2_TRACKED_COLS: dict[str, list[str]] = {
    "dim_branch":           ["branch_name", "city", "state"],
    "dim_account_customer": ["account_type", "branch_name", "has_active_card"],
}

SCD2_NATURAL_KEYS: dict[str, str] = {
    "dim_branch":           "branch_id",
    "dim_account_customer": "account_id",
}

SCD2_SURROGATE_KEYS: dict[str, str] = {
    "dim_branch":           "branch_sk",
    "dim_account_customer": "account_sk",
}
```

Also replace the existing `PK_MAP` with the expanded version:

```python
PK_MAP = {
    DIM_TABLE:        "account_sk",
    FACT_TABLE:       "transaction_id",
    DIM_DATE_TABLE:   "date_sk",
    DIM_BRANCH_TABLE: "branch_sk",
}
```

- [ ] **Step 2: Verify the module imports without error**

```bash
source .venv/bin/activate && python -c "from src.config import SCD2_TABLES, SCD2_TRACKED_COLS, DIM_DATE_TABLE; print('OK')"
```

Expected output: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/config.py
git commit -m "feat: add SCD2 config constants and new dim table names"
```

---

## Task 2: `build_dim_date()` with TDD

**Files:**
- Create: `tests/test_dim_date.py`
- Modify: `src/transform.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_dim_date.py`:

```python
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
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_dim_date.py -v
```

Expected: `FAILED` with `ImportError: cannot import name 'build_dim_date'`

- [ ] **Step 3: Implement `build_dim_date()` in `src/transform.py`**

Add this function after the existing imports, before `parse_mapping_sheet`:

```python
def build_dim_date(parquet_dir: Path) -> pd.DataFrame:
    """
    Generate dim_date from min/max txn_timestamp in transactions.parquet.
    dim_date is immutable — regenerated fresh on every pipeline run.
    """
    txns = pd.read_parquet(parquet_dir / "transactions.parquet")
    dates = pd.to_datetime(txns["txn_timestamp"])
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
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
pytest tests/test_dim_date.py -v
```

Expected: `8 passed`

- [ ] **Step 5: Commit**

```bash
git add tests/test_dim_date.py src/transform.py
git commit -m "feat: add build_dim_date() with calendar dimension generation"
```

---

## Task 3: `build_dim_branch()` with TDD

**Files:**
- Modify: `tests/test_dim_date.py` (append branch tests)
- Modify: `src/transform.py`

- [ ] **Step 1: Add failing branch tests to `tests/test_dim_date.py`**

Append to `tests/test_dim_date.py`:

```python
from src.transform import build_dim_branch


@pytest.fixture
def branches_parquet(tmp_path):
    pd.DataFrame({
        "branch_id":   [1, 2, 3],
        "branch_name": ["North Branch", "South Branch", "East Branch"],
        "city":        ["Chennai", "Mumbai", "Delhi"],
        "state":       ["TN", "MH", "DL"],
        "country":     ["India", "India", "India"],
    }).to_parquet(tmp_path / "branches.parquet", index=False)
    return tmp_path


def test_dim_branch_row_count(branches_parquet):
    df = build_dim_branch(branches_parquet)
    assert len(df) == 3


def test_dim_branch_columns(branches_parquet):
    df = build_dim_branch(branches_parquet)
    expected = {"branch_id", "branch_name", "city", "state", "country"}
    assert expected.issubset(set(df.columns))


def test_dim_branch_no_sk_column(branches_parquet):
    """branch_sk is assigned by load_scd2, not the builder."""
    df = build_dim_branch(branches_parquet)
    assert "branch_sk" not in df.columns


def test_dim_branch_sorted_by_branch_id(branches_parquet):
    df = build_dim_branch(branches_parquet)
    assert list(df["branch_id"]) == sorted(df["branch_id"].tolist())
```

- [ ] **Step 2: Run new tests — expect FAIL**

```bash
pytest tests/test_dim_date.py::test_dim_branch_row_count -v
```

Expected: `FAILED` with `ImportError: cannot import name 'build_dim_branch'`

- [ ] **Step 3: Implement `build_dim_branch()` in `src/transform.py`**

Add after `build_dim_date()`:

```python
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
```

- [ ] **Step 4: Run all dim tests — expect PASS**

```bash
pytest tests/test_dim_date.py -v
```

Expected: `12 passed`

- [ ] **Step 5: Commit**

```bash
git add tests/test_dim_date.py src/transform.py
git commit -m "feat: add build_dim_branch() returning natural-key snapshot"
```

---

## Task 4: `load_scd2()` with TDD

**Files:**
- Create: `tests/test_scd2.py`
- Modify: `src/load.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_scd2.py`:

```python
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
    # Must be > 2 (the max from initial load)
    assert new_sk > 2
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_scd2.py -v
```

Expected: `FAILED` with `ImportError: cannot import name 'load_scd2'`

- [ ] **Step 3: Implement `load_scd2()` in `src/load.py`**

Add after `load_table_dedup()`. Also add `from datetime import date as _date, timedelta` at the top of the file.

```python
def load_scd2(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
    natural_key: str,
    tracked_cols: list[str],
    surrogate_key_col: str | None = None,
    today: "_date | None" = None,
) -> None:
    """
    SCD Type 2 upsert: tracks history for tracked_cols, expires old rows,
    inserts new versions. Assigns surrogate keys if surrogate_key_col is given.

    Non-tracked columns are Type 1 (overwritten in place for unchanged rows).
    """
    from datetime import date as _date, timedelta
    today = today or _date.today()
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
        if any(str(inc_row[c]) != str(cur_row[c]) for c in tracked_cols):
            changed_keys.append(key)

    # 1. Expire changed rows
    for key in changed_keys:
        conn.execute(f"""
            UPDATE {table_name}
            SET effective_to = DATE '{yesterday.isoformat()}', is_current = FALSE
            WHERE "{natural_key}" = {key!r} AND is_current = TRUE
        """)

    # 2. Insert new versions for changed rows + new rows
    keys_to_insert = set(changed_keys) | new_keys
    if keys_to_insert:
        rows = df[df[natural_key].isin(keys_to_insert)].copy()
        rows["effective_from"] = pd.Timestamp(today)
        rows["effective_to"]   = pd.NaT
        rows["is_current"]     = True

        versions = []
        for _, row in rows.iterrows():
            key = row[natural_key]
            if key in changed_keys:
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

        conn.execute(f"INSERT INTO {table_name} SELECT * FROM rows")

    # 3. Type 1 overwrite for non-tracked columns on UNCHANGED rows
    # (columns not in tracked_cols may still change — update them in place)
    unchanged_keys = candidate_keys - set(changed_keys)
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
            val_repr = f"'{val}'" if isinstance(val, str) else repr(val)
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
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
pytest tests/test_scd2.py -v
```

Expected: `12 passed`

- [ ] **Step 5: Run full test suite — no regressions**

```bash
pytest tests/ -v
```

Expected: all previously passing tests still pass

- [ ] **Step 6: Commit**

```bash
git add tests/test_scd2.py src/load.py
git commit -m "feat: add load_scd2() with version history, expiry, and SK auto-assignment"
```

---

## Task 5: Upgrade `build_fact_transactions()` for `date_sk` and `branch_sk`

**Files:**
- Modify: `src/transform.py`
- Modify: `tests/test_transform.py`

- [ ] **Step 1: Add failing tests for new FKs**

Append to `tests/test_transform.py`:

```python
from pathlib import Path
from src.transform import build_fact_transactions, build_dim_date


@pytest.fixture
def star_parquet(tmp_path):
    """Synthetic parquet directory for fact builder tests."""
    import pandas as pd

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
    dim_df       = pd.DataFrame({"account_sk": [1, 2], "account_id": [10, 20]})
    dim_date_df  = build_dim_date(star_parquet)
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
```

- [ ] **Step 2: Run new tests — expect FAIL**

```bash
pytest tests/test_transform.py::test_fact_has_date_sk -v
```

Expected: `FAILED` — `build_fact_transactions() takes 2 positional arguments but 4 were given`

- [ ] **Step 3: Update `build_fact_transactions()` in `src/transform.py`**

Replace the existing `build_fact_transactions()` function with:

```python
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
```

- [ ] **Step 4: Run all transform tests — expect PASS**

```bash
pytest tests/test_transform.py -v
```

Expected: all tests pass (original 6 + 3 new = 9 passed)

- [ ] **Step 5: Commit**

```bash
git add tests/test_transform.py src/transform.py
git commit -m "feat: upgrade build_fact_transactions() with date_sk and branch_sk FKs"
```

---

## Task 6: Hybrid engine + enricher registry

**Files:**
- Create: `tests/test_hybrid_engine.py`
- Modify: `src/transform.py` (add `ENRICHER_REGISTRY`, `run_hybrid_transforms()`, update `parse_mapping_sheet()`)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_hybrid_engine.py`:

```python
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


def test_cast_date_rule(branch_parquet):
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
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
pytest tests/test_hybrid_engine.py -v
```

Expected: `FAILED` with `ImportError: cannot import name 'run_hybrid_transforms'`

- [ ] **Step 3: Update `parse_mapping_sheet()` to read the two new columns**

In `src/transform.py`, inside `parse_mapping_sheet()`, find the `rule` dict and replace it with:

```python
rule = {
    "target_col":     str(row[1]).strip() if row[1] else "",
    "target_dtype":   str(row[2]).strip() if row[2] else "",
    "source_table":   str(row[3]).strip() if row[3] else "None",
    "source_col":     str(row[4]).strip() if row[4] else "None",
    "transform_rule": str(row[5]).strip() if row[5] else "",
    "enricher":       str(row[6]).strip() if len(row) > 6 and row[6] and str(row[6]).strip() not in ("None", "") else "",
    "scd_type":       int(row[7]) if len(row) > 7 and row[7] and str(row[7]).strip().isdigit() else 1,
}
```

- [ ] **Step 4: Add `ENRICHER_REGISTRY` and `run_hybrid_transforms()` to `src/transform.py`**

Add after the existing builder functions (`build_dim_account_customer`, `build_fact_transactions`):

```python
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

ENRICHER_REGISTRY: dict[str, callable] = {
    "generate_date_dim": lambda parquet_dir, ctx: build_dim_date(parquet_dir),
    "build_dim_account_customer_enricher": lambda parquet_dir, ctx: build_dim_account_customer(parquet_dir),
}

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

    # Start from the first source table; subsequent tables would be joined here
    primary_table = next(iter(source_tables))
    df = source_dfs[primary_table].copy()

    result_cols: dict[str, pd.Series] = {}

    for rule in rules:
        if rule["enricher"]:
            continue  # handled in enricher pass

        src_table = rule["source_table"]
        src_col   = rule["source_col"]
        tgt_col   = rule["target_col"]
        transform = rule["transform_rule"].upper()
        src_df    = source_dfs.get(src_table, df)

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
            progress_callback(f"Building {target_table}", step, total)

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
```

- [ ] **Step 5: Run hybrid engine tests — expect PASS**

```bash
pytest tests/test_hybrid_engine.py -v
```

Expected: `5 passed`

- [ ] **Step 6: Run full suite — no regressions**

```bash
pytest tests/ -v
```

Expected: all tests pass

- [ ] **Step 7: Commit**

```bash
git add tests/test_hybrid_engine.py src/transform.py
git commit -m "feat: add hybrid engine with ENRICHER_REGISTRY and run_hybrid_transforms()"
```

---

## Task 7: Update the mapping sheet Excel

**Files:**
- Modify: `DataBridge mapping sheet.xlsx`

- [ ] **Step 1: Run the update script**

Create a one-off script at the project root (delete after running):

```python
# update_mapping_sheet.py — run once then delete
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from pathlib import Path

path = Path("DataBridge mapping sheet.xlsx")
wb   = openpyxl.load_workbook(path)
ws   = wb["Data_Mapping_sheet"]

# ── Add header columns G and H ──────────────────────
ws["G1"] = "enricher"
ws["H1"] = "scd_type"
for cell in [ws["G1"], ws["H1"]]:
    cell.font      = Font(bold=True)
    cell.alignment = Alignment(horizontal="center")

# ── Backfill scd_type = 1 for existing non-header rows ──
for row in ws.iter_rows(min_row=2):
    if any(cell.value for cell in row[:6]):
        row[7].value = 1  # column H

# ── Helper to append a block of rows ────────────────
def append_rows(ws, rows):
    for row_data in rows:
        ws.append(row_data)

# ── dim_date rows (scd_type=1, table-builder enricher) ──
append_rows(ws, [
    ["dim_date", "date_sk",     "INTEGER", "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "full_date",   "DATE",    "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "day_of_week", "INTEGER", "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "day_name",    "VARCHAR", "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "week_number", "INTEGER", "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "month",       "INTEGER", "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "month_name",  "VARCHAR", "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "quarter",     "INTEGER", "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "year",        "INTEGER", "—", "—", "DERIVED", "generate_date_dim", 1],
    ["",         "is_weekend",  "BOOLEAN", "—", "—", "DERIVED", "generate_date_dim", 1],
])

# ── dim_branch rows (scd_type=2, DIRECT rules) ──────
append_rows(ws, [
    ["dim_branch", "branch_id",   "INTEGER", "branches", "branch_id",   "DIRECT", "", 2],
    ["",           "branch_name", "VARCHAR", "branches", "branch_name", "DIRECT", "", 2],
    ["",           "city",        "VARCHAR", "branches", "city",        "DIRECT", "", 2],
    ["",           "state",       "VARCHAR", "branches", "state",       "DIRECT", "", 2],
    ["",           "country",     "VARCHAR", "branches", "country",     "DIRECT", "", 2],
])

# ── Add date_sk and branch_sk to fact_transactions ──
# Find last row of fact_transactions block and append
append_rows(ws, [
    ["fact_transactions", "date_sk",   "INTEGER", "dim_date",   "date_sk",   "CAST:INT", "", 1],
    ["",                  "branch_sk", "INTEGER", "dim_branch", "branch_sk", "CAST:INT", "", 1],
])

wb.save(path)
print("Mapping sheet updated.")
```

```bash
source .venv/bin/activate && python update_mapping_sheet.py
```

Expected: `Mapping sheet updated.`

- [ ] **Step 2: Verify the sheet opens correctly**

```bash
python -c "
from src.transform import parse_mapping_sheet
m = parse_mapping_sheet('DataBridge mapping sheet.xlsx')
print('Tables:', list(m['mappings'].keys()))
print('dim_date rules:', len(m['mappings'].get('dim_date', [])))
print('dim_branch rules:', len(m['mappings'].get('dim_branch', [])))
print('OK')
"
```

Expected output includes `dim_date` and `dim_branch` in the tables list.

- [ ] **Step 3: Add `build_dim_account_customer_enricher` to existing dim_account_customer rows**

The existing `dim_account_customer` rows in the sheet need the enricher set so the hybrid engine knows to call the named builder (not attempt generic DIRECT rules, which can't handle multi-table joins). Add this block to `update_mapping_sheet.py` before the `wb.save(path)` line:

```python
# Update existing dim_account_customer rows to add the enricher
for row in ws.iter_rows(min_row=2):
    target_table = str(row[0].value).strip() if row[0].value else ""
    target_col   = str(row[1].value).strip() if row[1].value else ""
    if target_table == "dim_account_customer" and target_col == "account_sk":
        row[6].value = "build_dim_account_customer_enricher"
        row[7].value = 2
        break  # only need to tag one row; engine finds it via TABLE_BUILDER_ENRICHERS
```

- [ ] **Step 4: Delete the one-off script and commit**

```bash
rm update_mapping_sheet.py
git add "DataBridge mapping sheet.xlsx"
git commit -m "feat: add enricher/scd_type columns and dim_date/dim_branch rows to mapping sheet"
```

---

## Task 8: Update `load_star_schema()` and `pipeline.py`

**Files:**
- Modify: `src/load.py`
- Modify: `src/pipeline.py`

- [ ] **Step 1: Replace `load_star_schema()` in `src/load.py`**

Replace the existing `load_star_schema()` function:

```python
def load_star_schema(
    dim_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    dim_branch_df: pd.DataFrame,
    dim_date_df: pd.DataFrame,
    db_path: str | None = None,
    progress_callback=None,
) -> None:
    """
    Load all star-schema tables into DuckDB.

    Load order respects FK dependencies:
      1. dim_date  (immutable, replace)
      2. dim_branch (SCD Type 2)
      3. dim_account_customer (SCD Type 2)
      4. fact_transactions (replace)
    """
    from src.config import (
        DIM_DATE_TABLE, DIM_BRANCH_TABLE, DIM_TABLE, FACT_TABLE,
        SCD2_TRACKED_COLS, SCD2_NATURAL_KEYS, SCD2_SURROGATE_KEYS,
    )

    conn = get_duck_conn(db_path)

    if progress_callback:
        progress_callback("Loading dim_date", 1, 4)
    load_table(conn, dim_date_df, DIM_DATE_TABLE, mode="replace")
    log.info("  Loaded %s → %d rows", DIM_DATE_TABLE, len(dim_date_df))

    if progress_callback:
        progress_callback("Loading dim_branch (SCD2)", 2, 4)
    load_scd2(
        conn, dim_branch_df, DIM_BRANCH_TABLE,
        natural_key=SCD2_NATURAL_KEYS[DIM_BRANCH_TABLE],
        tracked_cols=SCD2_TRACKED_COLS[DIM_BRANCH_TABLE],
        surrogate_key_col=SCD2_SURROGATE_KEYS[DIM_BRANCH_TABLE],
    )

    if progress_callback:
        progress_callback("Loading dim_account_customer (SCD2)", 3, 4)
    load_scd2(
        conn, dim_df, DIM_TABLE,
        natural_key=SCD2_NATURAL_KEYS[DIM_TABLE],
        tracked_cols=SCD2_TRACKED_COLS[DIM_TABLE],
        surrogate_key_col=SCD2_SURROGATE_KEYS[DIM_TABLE],
    )

    if progress_callback:
        progress_callback("Loading fact_transactions", 4, 4)
    load_table(conn, fact_df, FACT_TABLE, mode="replace")
    log.info("  Loaded %s → %d rows", FACT_TABLE, len(fact_df))

    conn.close()
    log.info("Galaxy schema loaded successfully into DuckDB")
```

- [ ] **Step 2: Update `run_mapping_pipeline()` in `src/pipeline.py`**

Replace the body of `run_mapping_pipeline()` with:

```python
def run_mapping_pipeline(
    mapping_sheet_path: str | Path,
    db_path: str | None = None,
    progress_callback=None,
) -> PipelineRunMeta:
    """Execute the three-phase mapping-sheet-driven ETL pipeline (galaxy schema)."""
    from src.transform import (
        parse_mapping_sheet, get_required_source_tables,
        run_hybrid_transforms, build_fact_transactions,
    )
    from src.load import load_all, load_all_staged, load_star_schema, get_duck_conn

    meta = PipelineRunMeta(
        run_id=datetime.now().strftime("%Y%m%d_%H%M%S"),
        started_at=datetime.now(),
    )

    def _progress(phase: str, msg: str, pct: float):
        meta.phase  = phase
        meta.status = phase.lower().replace(" ", "_")
        if progress_callback:
            progress_callback(phase, msg, pct)
        log.info("  [%s] %s (%.0f%%)", phase, msg, pct * 100)

    try:
        # ── Phase 1: EXTRACT ──────────────────────────
        _progress("Extract", "Parsing mapping sheet…", 0.05)
        mapping      = parse_mapping_sheet(mapping_sheet_path)
        source_tables = get_required_source_tables(mapping)

        _progress("Extract", "Connecting to PostgreSQL…", 0.10)
        engine = get_pg_engine()

        _progress("Extract", f"Extracting {len(source_tables)} tables…", 0.15)
        parquet_dir = STAGING_DIR
        manifest = extract_tables_to_parquet(
            engine, source_tables, parquet_dir,
            progress_callback=lambda name, idx, total: _progress(
                "Extract", f"Extracted {name} ({idx}/{total})",
                0.15 + 0.20 * idx / total,
            ),
        )
        meta.tables_extracted = list(manifest.keys())
        meta.parquet_files    = [str(p) for p in manifest.values()]

        # ── Phase 2: TRANSFORM dims ───────────────────
        _progress("Transform", "Building dimension tables…", 0.40)
        dim_results = run_hybrid_transforms(
            parquet_dir, mapping,
            progress_callback=lambda name, step, total: _progress(
                "Transform", name, 0.40 + 0.15 * step / total,
            ),
        )
        dim_df        = dim_results.get("dim_account_customer", None)
        dim_branch_df = dim_results.get("dim_branch", None)
        dim_date_df   = dim_results.get("dim_date", None)

        # ── Phase 3a: LOAD dims ───────────────────────
        _progress("Load", "Loading dimension tables…", 0.60)
        target_db = db_path or DUCKDB_PATH

        # Temporarily call load_star_schema without fact to load dims
        # then query current-state SKs before building the fact table.
        conn = get_duck_conn(target_db)
        from src.load import load_table, load_scd2
        from src.config import (
            DIM_DATE_TABLE, DIM_BRANCH_TABLE, DIM_TABLE,
            SCD2_TRACKED_COLS, SCD2_NATURAL_KEYS, SCD2_SURROGATE_KEYS,
        )

        load_table(conn, dim_date_df, DIM_DATE_TABLE, mode="replace")
        load_scd2(conn, dim_branch_df, DIM_BRANCH_TABLE,
                  natural_key=SCD2_NATURAL_KEYS[DIM_BRANCH_TABLE],
                  tracked_cols=SCD2_TRACKED_COLS[DIM_BRANCH_TABLE],
                  surrogate_key_col=SCD2_SURROGATE_KEYS[DIM_BRANCH_TABLE])
        load_scd2(conn, dim_df, DIM_TABLE,
                  natural_key=SCD2_NATURAL_KEYS[DIM_TABLE],
                  tracked_cols=SCD2_TRACKED_COLS[DIM_TABLE],
                  surrogate_key_col=SCD2_SURROGATE_KEYS[DIM_TABLE])

        # ── Phase 3b: Build fact with current-state SKs ──
        _progress("Transform", "Building fact table with SK lookups…", 0.72)
        dim_lookup = conn.execute(
            f"SELECT account_sk, account_id FROM {DIM_TABLE} WHERE is_current = TRUE"
        ).fetchdf()
        branch_lookup = conn.execute(
            f"SELECT branch_sk, branch_id FROM {DIM_BRANCH_TABLE} WHERE is_current = TRUE"
        ).fetchdf()

        fact_df = build_fact_transactions(
            parquet_dir, dim_lookup, dim_date_df, branch_lookup
        )
        meta.dim_row_count  = len(dim_df)
        meta.fact_row_count = len(fact_df)

        # ── Phase 3c: LOAD fact ───────────────────────
        _progress("Load", "Loading fact table…", 0.85)
        from src.config import FACT_TABLE as _FACT_TABLE
        load_table(conn, fact_df, _FACT_TABLE, mode="replace")
        conn.close()

        meta.status      = "done"
        meta.finished_at = datetime.now()
        _progress("Done", "Pipeline complete!", 1.0)
        log.info(
            "Pipeline finished in %.1fs — dim: %d rows, fact: %d rows",
            meta.duration_seconds, meta.dim_row_count, meta.fact_row_count,
        )

    except Exception as exc:
        meta.status        = "failed"
        meta.error_message = str(exc)
        meta.finished_at   = datetime.now()
        log.error("Pipeline FAILED: %s", exc, exc_info=True)
        if progress_callback:
            progress_callback("Error", str(exc), 0.0)
        raise

    return meta
```

- [ ] **Step 3: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: all existing tests pass (pipeline changes are tested by integration test in Task 9)

- [ ] **Step 4: Commit**

```bash
git add src/load.py src/pipeline.py
git commit -m "feat: update load_star_schema() and pipeline for 4-table galaxy schema"
```

---

## Task 9: End-to-end integration test

**Files:**
- Create: `tests/test_pipeline_integration.py`

- [ ] **Step 1: Write the integration test**

Create `tests/test_pipeline_integration.py`:

```python
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
```

- [ ] **Step 2: Run integration tests — expect PASS**

```bash
pytest tests/test_pipeline_integration.py -v
```

Expected: `6 passed`

- [ ] **Step 3: Run full suite — no regressions**

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Expected: ~30 tests passing; coverage should be visibly higher than the original 11-test suite.

- [ ] **Step 4: Commit**

```bash
git add tests/test_pipeline_integration.py
git commit -m "test: add end-to-end integration test for galaxy schema pipeline"
```

---

## Task 10: Advanced SQL templates + live lineage in `app.py`

**Files:**
- Modify: `src/app.py`

- [ ] **Step 1: Add 8 new SQL Workbench templates**

In `src/app.py`, find the `template_options` dict inside the `📝 SQL Workbench` page and extend it with these entries (add after the existing entries, before the closing `}`):

```python
        # ── Dimensional joins ─────────────────────────
        "Transactions by Quarter (dim_date join)": (
            "SELECT d.year, d.quarter,\n"
            "       COUNT(*) AS txn_count,\n"
            "       ROUND(SUM(f.amount), 2) AS total_amount\n"
            "FROM fact_transactions f\n"
            "JOIN dim_date d ON f.date_sk = d.date_sk\n"
            "GROUP BY d.year, d.quarter\n"
            "ORDER BY d.year, d.quarter"
        ),
        "Branch performance (three-way join)": (
            "SELECT b.branch_name, b.city,\n"
            "       COUNT(*) AS txns,\n"
            "       ROUND(SUM(f.amount), 2) AS volume\n"
            "FROM fact_transactions f\n"
            "JOIN dim_account_customer a ON f.account_sk = a.account_sk\n"
            "JOIN dim_branch b ON f.branch_sk = b.branch_sk\n"
            "WHERE b.is_current = TRUE\n"
            "GROUP BY b.branch_name, b.city\n"
            "ORDER BY volume DESC"
        ),
        # ── Window functions ──────────────────────────
        "Running total per account (LAG + cumulative SUM)": (
            "SELECT transaction_id, account_sk,\n"
            "       transaction_date, amount,\n"
            "       SUM(amount) OVER (\n"
            "           PARTITION BY account_sk\n"
            "           ORDER BY transaction_date\n"
            "       ) AS running_total,\n"
            "       amount - LAG(amount, 1, 0) OVER (\n"
            "           PARTITION BY account_sk\n"
            "           ORDER BY transaction_date\n"
            "       ) AS delta_from_prev\n"
            "FROM fact_transactions\n"
            "ORDER BY account_sk, transaction_date"
        ),
        "Account rank per branch (RANK OVER)": (
            "SELECT a.branch_name, a.customer_name,\n"
            "       ROUND(SUM(f.amount), 2) AS total_spent,\n"
            "       RANK() OVER (\n"
            "           PARTITION BY a.branch_name\n"
            "           ORDER BY SUM(f.amount) DESC\n"
            "       ) AS branch_rank\n"
            "FROM fact_transactions f\n"
            "JOIN dim_account_customer a ON f.account_sk = a.account_sk\n"
            "WHERE a.is_current = TRUE\n"
            "GROUP BY a.branch_name, a.customer_name\n"
            "ORDER BY a.branch_name, branch_rank"
        ),
        # ── CTE ───────────────────────────────────────
        "Monthly cohort summary (CTE)": (
            "WITH monthly AS (\n"
            "    SELECT d.year, d.month, d.month_name,\n"
            "           COUNT(*) AS txn_count,\n"
            "           ROUND(SUM(f.amount), 2) AS total_amount,\n"
            "           ROUND(AVG(f.amount), 2) AS avg_amount\n"
            "    FROM fact_transactions f\n"
            "    JOIN dim_date d ON f.date_sk = d.date_sk\n"
            "    GROUP BY d.year, d.month, d.month_name\n"
            ")\n"
            "SELECT * FROM monthly ORDER BY year, month"
        ),
        # ── SCD Type 2 history queries ────────────────
        "SCD2: Full version history for account_id = 1": (
            "SELECT account_id, account_type, branch_name, has_active_card,\n"
            "       version, effective_from, effective_to, is_current\n"
            "FROM dim_account_customer\n"
            "WHERE account_id = 1\n"
            "ORDER BY version"
        ),
        "SCD2: Accounts that changed type this year": (
            "SELECT account_id, customer_name, account_type, effective_from\n"
            "FROM dim_account_customer\n"
            "WHERE version > 1\n"
            "  AND EXTRACT(year FROM CAST(effective_from AS DATE)) = EXTRACT(year FROM CURRENT_DATE)\n"
            "ORDER BY effective_from DESC"
        ),
        "SCD2: Point-in-time branch lookup for 2024-03-01": (
            "SELECT branch_id, branch_name, city, state,\n"
            "       effective_from, effective_to, is_current\n"
            "FROM dim_branch\n"
            "WHERE effective_from <= DATE '2024-03-01'\n"
            "  AND (effective_to > DATE '2024-03-01' OR effective_to IS NULL)\n"
            "ORDER BY branch_id"
        ),
```

- [ ] **Step 2: Replace the static lineage lists with a live mapping-sheet parse**

In `src/app.py`, find the `📋 Data Lineage` page section. Replace the hardcoded `dim_mappings` and `fact_mappings` lists with:

```python
    # ── Live mapping from uploaded sheet ─────────────
    mapping_data = st.session_state.get("parsed_mapping", None)

    if mapping_data:
        for target_table, rules in mapping_data["mappings"].items():
            icon = "🔷" if target_table.startswith("dim_") else "🔶"
            st.markdown(f"### {icon} {target_table}")
            rules_df = pd.DataFrame(rules)
            st.dataframe(
                rules_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "target_col":     st.column_config.TextColumn("Target Column", width="medium"),
                    "target_dtype":   st.column_config.TextColumn("Type", width="small"),
                    "source_table":   st.column_config.TextColumn("Source Table", width="medium"),
                    "source_col":     st.column_config.TextColumn("Source Column", width="medium"),
                    "transform_rule": st.column_config.TextColumn("Transform Rule", width="medium"),
                    "enricher":       st.column_config.TextColumn("Enricher", width="medium"),
                    "scd_type":       st.column_config.TextColumn("SCD Type", width="small"),
                },
            )
            st.markdown("---")
    else:
        st.info(
            "Upload a mapping sheet on the **🚀 Pipeline Control** page to see live lineage. "
            "Showing fallback static view below."
        )
        # ── Fallback static view (original hardcoded data) ──
        dim_mappings = [
            {"Target": "account_sk",      "Type": "INTEGER", "Source": "—",                        "Transform": "Auto-generated surrogate key"},
            {"Target": "account_id",      "Type": "INTEGER", "Source": "accounts.account_id",       "Transform": "Direct mapping (Natural Key)"},
            {"Target": "customer_id",     "Type": "INTEGER", "Source": "customers.customer_id",     "Transform": "INNER JOIN accounts ↔ customers"},
            {"Target": "customer_name",   "Type": "VARCHAR", "Source": "customers.full_name",       "Transform": "Direct mapping (renamed)"},
            {"Target": "customer_state",  "Type": "VARCHAR", "Source": "customers.state",           "Transform": "Direct mapping (renamed)"},
            {"Target": "branch_name",     "Type": "VARCHAR", "Source": "branches.branch_name",      "Transform": "LEFT JOIN on branch_id"},
            {"Target": "account_type",    "Type": "VARCHAR", "Source": "accounts.account_type",     "Transform": "Direct mapping"},
            {"Target": "has_active_card", "Type": "BOOLEAN", "Source": "cards.is_active",           "Transform": "Derived: ANY(is_active) grouped by account_id"},
            {"Target": "effective_from",  "Type": "DATE",    "Source": "—",                        "Transform": "SCD2: date row became current"},
            {"Target": "effective_to",    "Type": "DATE",    "Source": "—",                        "Transform": "SCD2: date row expired (NULL = current)"},
            {"Target": "is_current",      "Type": "BOOLEAN", "Source": "—",                        "Transform": "SCD2: TRUE for current version"},
            {"Target": "version",         "Type": "INTEGER", "Source": "—",                        "Transform": "SCD2: version number (1 = original)"},
        ]
        fact_mappings = [
            {"Target": "transaction_id",  "Type": "INTEGER", "Source": "transactions.transaction_id", "Transform": "Direct mapping"},
            {"Target": "account_sk",      "Type": "INTEGER", "Source": "dim_account_customer",        "Transform": "Lookup: account_id → current account_sk"},
            {"Target": "date_sk",         "Type": "INTEGER", "Source": "dim_date",                    "Transform": "Lookup: transaction_date → date_sk"},
            {"Target": "branch_sk",       "Type": "INTEGER", "Source": "dim_branch",                  "Transform": "Lookup: account→branch_id → branch_sk"},
            {"Target": "transaction_date","Type": "DATE",    "Source": "transactions.txn_timestamp",   "Transform": "Cast: TIMESTAMP → DATE"},
            {"Target": "transaction_type","Type": "VARCHAR", "Source": "transaction_types.type_name",  "Transform": "LEFT JOIN on type_id"},
            {"Target": "amount",          "Type": "DECIMAL", "Source": "transactions.amount",          "Transform": "Direct mapping"},
        ]
        st.markdown("### 🔷 dim_account_customer")
        st.dataframe(pd.DataFrame(dim_mappings), use_container_width=True, hide_index=True)
        st.markdown("---")
        st.markdown("### 🔶 fact_transactions")
        st.dataframe(pd.DataFrame(fact_mappings), use_container_width=True, hide_index=True)
```

Also, in the **Pipeline Control** page, after the mapping sheet is parsed successfully, save it to `session_state`. Find the line `st.markdown("#### ✅ Mapping Parsed Successfully")` and add directly before it:

```python
                st.session_state["parsed_mapping"] = mapping
```

- [ ] **Step 3: Run the full test suite — all green**

```bash
pytest tests/ -v
```

Expected: all ~30 tests pass

- [ ] **Step 4: Verify the app launches without import errors**

```bash
source .venv/bin/activate && python -c "import src.app; print('app imports OK')"
```

Expected: `app imports OK`

- [ ] **Step 5: Commit**

```bash
git add src/app.py
git commit -m "feat: add 8 advanced SQL templates and live lineage page to Streamlit dashboard"
```

---

## Final Verification

- [ ] **Run the full test suite with coverage**

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Expected: ~30 tests, all passing. Coverage report shows `load.py`, `transform.py`, `config.py` all well-covered.

- [ ] **Confirm all 10 files changed**

```bash
git log --oneline -10
```

Expected: 10 commits, one per task.

- [ ] **Confirm the galaxy schema is academically complete**

```bash
python -c "
from src.config import (SCD2_TABLES, SCD2_TRACKED_COLS, DIM_DATE_TABLE,
                        DIM_BRANCH_TABLE, SCD2_SURROGATE_KEYS)
print('SCD2 tables:', SCD2_TABLES)
print('dim_date table:', DIM_DATE_TABLE)
print('Surrogate keys:', SCD2_SURROGATE_KEYS)
print('All config OK')
"
```

Expected: prints all three config values without error.
