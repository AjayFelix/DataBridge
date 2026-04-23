"""
Centralized Configuration
─────────────────────────
All environment variable loading and path constants in one place.
Pipeline run metadata tracking via dataclass.
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── PostgreSQL (Source OLTP) ──────────────────────────
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "")
PG_USER = os.getenv("PG_USER", "")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

# ── DuckDB (Target OLAP) ─────────────────────────────
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/databridge.duckdb")

# ── Staging / Intermediate ────────────────────────────
STAGING_DIR = Path(os.getenv("STAGING_DIR", "data/staging"))
WATERMARK_PATH = Path(os.getenv("WATERMARK_PATH", "data/watermarks.json"))

# ── Target Table Names ────────────────────────────────
DIM_TABLE = "dim_account_customer"
FACT_TABLE = "fact_transactions"

# ── New Dimension Tables ──────────────────────────────
DIM_DATE_TABLE   = "dim_date"
DIM_BRANCH_TABLE = "dim_branch"

# ── Primary Keys ──────────────────────────────────────
PK_MAP = {
    DIM_TABLE:        "account_sk",
    FACT_TABLE:       "transaction_id",
    DIM_DATE_TABLE:   "date_sk",
    DIM_BRANCH_TABLE: "branch_sk",
}

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


@dataclass
class PipelineRunMeta:
    """Track metadata for a single pipeline run."""

    run_id: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None
    status: str = "pending"  # pending | extracting | transforming | loading | done | failed
    phase: str = ""
    error_message: str = ""
    tables_extracted: list[str] = field(default_factory=list)
    dim_row_count: int = 0
    fact_row_count: int = 0
    parquet_files: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None
