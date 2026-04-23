"""
Pipeline Orchestrator
─────────────────────
Three-phase ETL pipeline driven by the mapping sheet:

  Phase 1  EXTRACT   → PostgreSQL tables → Parquet (raw landing zone)
  Phase 2  TRANSFORM → Parquet → Star-schema DataFrames
  Phase 3  LOAD      → DataFrames → DuckDB

Supports both CLI and programmatic (Streamlit) invocation with
progress callbacks for real-time UI updates.
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from src.config import (
    STAGING_DIR, DUCKDB_PATH, PipelineRunMeta,
)
from src.extract import (
    get_pg_engine, extract_all, extract_all_incremental,
    extract_tables_to_parquet,
)
from src.transform import run_default_transforms
from src.load import load_all, load_all_staged, load_star_schema, load_table, load_scd2, get_duck_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
)
log = logging.getLogger("databridge")

# ── Watermark configuration ──────────────────────────
WATERMARK_MAP: dict[str, str] = {
    # "orders":    "updated_at",
    # "customers": "modified_date",
}


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
    from src.load import load_table, load_scd2, get_duck_conn

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

        conn = get_duck_conn(target_db)
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


# ── Legacy pipeline (raw table copy) ──────────────────

def run(
    mode: str = "replace",
    incremental: bool = False,
    use_staging: bool = False,
) -> None:
    """Execute the legacy full ETL pipeline (raw table copy)."""

    # ── Extract ───────────────────────────────────────
    log.info("── Extract ─────────────────────────────")
    engine = get_pg_engine()

    if incremental and WATERMARK_MAP:
        log.info("Mode: INCREMENTAL (watermark-based)")
        raw = extract_all_incremental(engine, WATERMARK_MAP)
    else:
        if incremental:
            log.warning(
                "Incremental flag set but WATERMARK_MAP is empty — "
                "falling back to full extraction"
            )
        log.info("Mode: FULL extraction")
        raw = extract_all(engine)

    log.info("Extracted %d table(s): %s", len(raw), list(raw.keys()))

    # ── Transform ─────────────────────────────────────
    log.info("── Transform ───────────────────────────")
    transformed = {
        name: run_default_transforms(df) for name, df in raw.items()
    }
    for name, df in transformed.items():
        log.info("  %s → %d rows, %d cols", name, len(df), len(df.columns))

    # ── Load ──────────────────────────────────────────
    log.info("── Load ────────────────────────────────")
    if use_staging:
        log.info("Using Parquet intermediate staging")
        load_all_staged(transformed, mode=mode)
    else:
        load_all(transformed, mode=mode)

    log.info("All tables loaded into DuckDB (mode=%s)", mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataBridge ETL Pipeline")
    parser.add_argument(
        "--mapping-sheet",
        type=str,
        default=None,
        help="Path to DataBridge mapping sheet (.xlsx) for star-schema pipeline",
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="Load mode: 'replace' (default) or 'append'",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use watermark-based incremental extraction",
    )
    parser.add_argument(
        "--staging",
        action="store_true",
        help="Stage to Parquet before loading into DuckDB",
    )
    args = parser.parse_args()

    if args.mapping_sheet:
        # New three-phase pipeline
        run_mapping_pipeline(args.mapping_sheet)
    else:
        # Legacy raw-copy pipeline
        run(mode=args.mode, incremental=args.incremental, use_staging=args.staging)
