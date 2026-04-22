# DataBridge Enhancement — Dimensional Depth Design Spec

**Date:** 2026-04-22
**Author:** AJAY FELIX
**Status:** Approved — ready for implementation

---

## 1. Goal

Enhance DataBridge from a simple 1-dim + 1-fact star schema into a proper **galaxy schema** with three dimensions, SCD Type 2 history tracking, a hybrid mapping-sheet-driven transform engine, and advanced SQL patterns in the workbench. The result demonstrates classical data warehousing concepts (surrogate keys, slowly changing dimensions, calendar tables, window functions, CTEs) in a working, end-to-end system.

---

## 2. Data Model

### 2.1 Target Schema (after enhancement)

```
dim_date ────────────────────────────────────┐
  date_sk        INTEGER  PK                  │
  full_date      DATE                         │
  day_of_week    INTEGER  (1=Mon … 7=Sun)     │
  day_name       VARCHAR                      │
  week_number    INTEGER                      │
  month          INTEGER                      │
  month_name     VARCHAR                      │
  quarter        INTEGER                      │
  year           INTEGER                      │
  is_weekend     BOOLEAN                      │
                                               ▼
dim_branch ──────────────┐     fact_transactions
  branch_sk  INTEGER PK  │       transaction_id  INTEGER  PK
  branch_id  INTEGER NK  │       account_sk      INTEGER  FK → dim_account_customer
  branch_name VARCHAR    │       date_sk          INTEGER  FK → dim_date
  city        VARCHAR    │       branch_sk        INTEGER  FK → dim_branch
  state       VARCHAR    │       transaction_type VARCHAR
  country     VARCHAR    │       amount           DECIMAL
  effective_from DATE    │
  effective_to   DATE    │
  is_current     BOOLEAN │
  [SCD Type 2]           │
                         │
dim_account_customer ────┘
  account_sk     INTEGER PK  (version-aware surrogate)
  account_id     INTEGER NK
  version        INTEGER
  customer_id    INTEGER
  customer_name  VARCHAR
  customer_state VARCHAR
  account_type   VARCHAR
  branch_name    VARCHAR
  has_active_card BOOLEAN
  effective_from DATE
  effective_to   DATE
  is_current     BOOLEAN
  [SCD Type 2]
```

### 2.2 Key design decisions

- **`dim_date`** is generated entirely in Python from the min/max of `txn_timestamp` in `transactions.parquet` (read directly — `fact_transactions` has not been built yet at this point in the pipeline). No PostgreSQL query is needed. It is immutable — rewritten with `mode="replace"` on every run.
- **`dim_branch`** is sourced from the `branches` PG table. SCD Type 2 tracks changes to `branch_name`, `city`, and `state`. Other columns (e.g. `country`) are Type 1 overwrites.
- **`dim_account_customer`** is upgraded from Type 1 to Type 2. Tracked columns: `account_type`, `branch_name`, `has_active_card`. Changes create a new version row; the old row is expired.
- **`fact_transactions`** gains two new foreign keys: `date_sk` (looked up from `dim_date` by transaction date) and `branch_sk` (looked up from `dim_branch` through the account's current branch).
- The `account_sk` foreign key in `fact_transactions` always points to the **historically correct** version of `dim_account_customer` at the time of the transaction. This is enforced by matching `transaction_date` against `effective_from` / `effective_to`.

---

## 3. Mapping Sheet Enhancement

Two new columns are added to the `Data_Mapping_sheet` tab in `DataBridge mapping sheet.xlsx`:

| Column | Values | Purpose |
|---|---|---|
| `enricher` | Python function name or empty | Names the registered enricher to call for complex derivations |
| `scd_type` | `1` or `2` | Marks whether this target table uses SCD Type 1 (overwrite) or Type 2 (versioned history) |

### Example rows for `dim_branch`

| target_col | target_dtype | source_table | source_col | transform_rule | enricher | scd_type |
|---|---|---|---|---|---|---|
| branch_sk | INTEGER | — | — | DERIVED | `generate_surrogate_key` | 2 |
| branch_id | INTEGER | branches | branch_id | DIRECT | — | 2 |
| branch_name | VARCHAR | branches | branch_name | DIRECT | — | 2 |
| city | VARCHAR | branches | city | DIRECT | — | 2 |
| state | VARCHAR | branches | state | DIRECT | — | 2 |
| country | VARCHAR | branches | country | DIRECT | — | 2 |
| effective_from | DATE | — | — | DERIVED | `scd2_effective_from` | 2 |
| effective_to | DATE | — | — | DERIVED | `scd2_effective_to` | 2 |
| is_current | BOOLEAN | — | — | DERIVED | `scd2_is_current` | 2 |

### Supported `transform_rule` values

| Rule | Behaviour |
|---|---|
| `DIRECT` | Copy `source_col` from `source_table` as-is |
| `RENAME` | Copy `source_col` and rename to `target_col` |
| `CAST:DATE` | Parse to `datetime.date` |
| `CAST:INT` | Cast to `int` |
| `CAST:BOOL` | Cast to `bool` |
| `DERIVED` | Delegate entirely to the named `enricher` function |

---

## 4. Hybrid Transform Engine

### 4.1 Location

All changes are in `src/transform.py`. Existing functions (`build_dim_account_customer`, `build_fact_transactions`) are **kept** and become the implementation bodies of enricher functions — they are no longer called directly by the orchestrator.

### 4.2 Enricher registry

```python
ENRICHER_REGISTRY: dict[str, callable] = {
    "generate_surrogate_key":    _generate_surrogate_key,
    "derive_has_active_card":    _derive_has_active_card,
    "generate_date_dim":         _generate_date_dim,
    "scd2_effective_from":       _scd2_effective_from,
    "scd2_effective_to":         _scd2_effective_to,
    "scd2_is_current":           _scd2_is_current,
    "lookup_date_sk":            _lookup_date_sk,
    "lookup_branch_sk":          _lookup_branch_sk,
}
```

Each enricher has the signature: `(df: pd.DataFrame, parquet_dir: Path, context: dict) -> pd.Series`.

The `context` dict carries shared state between enrichers for a single table build (e.g. the already-built `dim_date` DataFrame so `lookup_date_sk` can perform the join without re-reading files).

### 4.3 Two-pass orchestration

`run_hybrid_transforms(parquet_dir, mapping, progress_callback)` replaces `run_mapping_transforms()` as the public orchestration function:

**Pass 1 — Generic column mapper:**
Iterates over every rule for the target table. For `DIRECT`, `RENAME`, and `CAST:*` rules, applies the transformation inline using standard pandas operations. No enricher dispatch occurs.

**Pass 2 — Enricher dispatch:**
For every rule where `enricher` is non-empty, looks up the function in `ENRICHER_REGISTRY` and calls it. Raises `KeyError` with a clear message if the name is not registered.

### 4.4 Build order

Dimensions must be built before the fact table (fact needs `date_sk` and `branch_sk` lookups):

1. `dim_date` — generated from min/max of `txn_timestamp` in `transactions.parquet` (no PG source)
2. `dim_branch` — from `branches.parquet`
3. `dim_account_customer` — from `accounts`, `customers`, `branches`, `cards` parquet files
4. `fact_transactions` — from `transactions`, `transaction_types`; joins to all three dims for SK lookups

---

## 5. SCD Type 2 Load Strategy

### 5.1 New function: `load_scd2()`

Added to `src/load.py` alongside `load_table()` and `load_table_dedup()`.

**Signature:**
```python
def load_scd2(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
    natural_key: str,
    tracked_cols: list[str],
    today: date | None = None,
) -> None
```

**Algorithm:**

```
IF table does not exist:
    Add effective_from = today, effective_to = NULL, is_current = True, version = 1
    CREATE TABLE ... AS SELECT * FROM df
    RETURN

current = SELECT * FROM table WHERE is_current = True

CHANGED  = rows where natural_key matches but any tracked_col differs
UNCHANGED = rows where natural_key matches and all tracked_cols are identical
NEW      = rows in df whose natural_key is not in current

FOR each CHANGED row:
    UPDATE table SET effective_to = today - 1 day, is_current = False
      WHERE natural_key = row.natural_key AND is_current = True
    INSERT new row: version = old_version + 1,
                    effective_from = today,
                    effective_to = NULL,
                    is_current = True

FOR each NEW row:
    INSERT: version = 1, effective_from = today,
            effective_to = NULL, is_current = True

FOR each UNCHANGED row:
    Skip entirely.
```

**Type 1 columns** (not in `tracked_cols`) are updated in-place on the current row via a `SET` — no new version created.

### 5.2 SCD configuration in `config.py`

```python
SCD2_TABLES = {"dim_branch", "dim_account_customer"}

SCD2_TRACKED_COLS = {
    "dim_branch":           ["branch_name", "city", "state"],
    "dim_account_customer": ["account_type", "branch_name", "has_active_card"],
}

SCD2_NATURAL_KEYS = {
    "dim_branch":           "branch_id",
    "dim_account_customer": "account_id",
}
```

### 5.3 Updated `load_star_schema()`

Load order must respect FK dependencies:

```python
def load_star_schema(dim_df, fact_df, dim_branch_df, dim_date_df, db_path, ...):
    load_table(conn, dim_date_df, DIM_DATE_TABLE, mode="replace")          # immutable
    load_scd2(conn, dim_branch_df, DIM_BRANCH_TABLE, ...)                  # SCD2
    load_scd2(conn, dim_df, DIM_TABLE, ...)                                # SCD2
    load_table(conn, fact_df, FACT_TABLE, mode="replace")                  # fact last
```

---

## 6. Advanced SQL Workbench Templates

Eight new templates added to `app.py`, organized by concept:

### Dimensional joins
- **Transactions by Quarter** — `fact_transactions JOIN dim_date` grouped by `year`, `quarter`
- **Branch performance** — three-way join `fact → dim_account_customer → dim_branch`, `is_current = True` filter

### Window functions
- **Running total per account** — `SUM(amount) OVER (PARTITION BY account_sk ORDER BY transaction_date)` plus `LAG(amount)` delta
- **Account rank per branch** — `RANK() OVER (PARTITION BY branch_name ORDER BY SUM(amount) DESC)`

### CTE patterns
- **Monthly cohort summary** — CTE pre-aggregates by `year`, `month` from `dim_date` join

### SCD Type 2 history queries
- **Full version history for one account** — filter by `account_id`, order by `version`
- **Accounts that changed type this year** — `version > 1 AND effective_from >= current_year`
- **Point-in-time lookup** — find the correct dim row for a historical transaction date using `effective_from <= date AND (effective_to > date OR effective_to IS NULL)`

---

## 7. Data Lineage Page Update

The static hardcoded Python lists in the `📋 Data Lineage` page are replaced by a live read of the mapping sheet (re-parsed from the uploaded file stored in `st.session_state`). If no mapping sheet is in session, the existing static fallback is shown. This means the lineage view automatically reflects the new `dim_date` and `dim_branch` tables without any code change.

---

## 8. Testing

### New test files

| File | Tests |
|---|---|
| `tests/test_dim_date.py` | Date range coverage, `is_weekend` correctness, quarter/week accuracy, no duplicate `date_sk` |
| `tests/test_scd2.py` | Initial load, unchanged rows skipped, changed tracked col → new version + expiry, new natural key → inserted at version 1, Type 1 col change → in-place overwrite |
| `tests/test_hybrid_engine.py` | `DIRECT` rule, `RENAME` rule, `CAST:DATE` rule, enricher dispatch, unknown enricher raises `KeyError` |
| `tests/test_pipeline_integration.py` | End-to-end with synthetic Parquet in `tmp_path`: all 4 tables produced, FK integrity (`date_sk`, `branch_sk`), non-zero row counts |

### Coverage target

Total test count: **~30 tests** (up from 11). All tests run without PostgreSQL using in-memory DuckDB and `tmp_path` synthetic Parquet files.

---

## 9. Files Changed

| File | Change |
|---|---|
| `src/config.py` | Add `DIM_DATE_TABLE`, `DIM_BRANCH_TABLE`, `SCD2_TABLES`, `SCD2_TRACKED_COLS`, `SCD2_NATURAL_KEYS`, expand `PK_MAP` |
| `src/transform.py` | Add `ENRICHER_REGISTRY`, `run_hybrid_transforms()`, enricher functions, `build_dim_date()`, `build_dim_branch()` |
| `src/load.py` | Add `load_scd2()`, update `load_star_schema()` signature and body |
| `src/pipeline.py` | Update `run_mapping_pipeline()` to pass new dims to `load_star_schema()` |
| `src/app.py` | 8 new SQL templates, live lineage page |
| `DataBridge mapping sheet.xlsx` | Add `enricher` + `scd_type` columns, add rows for `dim_date` and `dim_branch` |
| `tests/test_dim_date.py` | New |
| `tests/test_scd2.py` | New |
| `tests/test_hybrid_engine.py` | New |
| `tests/test_pipeline_integration.py` | New |

---

## 10. What this demonstrates academically

| Concept | Where |
|---|---|
| Star schema → galaxy schema | 3 dims + 1 fact with proper FK relationships |
| Calendar / date dimension | `dim_date` generated from data range, no PG source |
| SCD Type 1 | In-place overwrite for non-tracked columns |
| SCD Type 2 | Versioned history with `effective_from/to`, `is_current` |
| Surrogate keys | `branch_sk`, `date_sk` — decoupled from natural keys |
| Data-driven transforms | Hybrid engine reads mapping sheet; adding a dim = updating Excel |
| Window functions | `SUM OVER`, `LAG`, `RANK OVER` in Workbench templates |
| CTEs | Multi-step aggregation without subquery nesting |
| Point-in-time queries | Historically correct lookups using SCD2 date ranges |
| FK integrity validation | Integration test verifies all SKs resolve across tables |
