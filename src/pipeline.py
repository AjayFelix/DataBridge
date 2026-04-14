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
from src.transform import (
    parse_mapping_sheet, get_required_source_tables,
    run_mapping_transforms, run_default_transforms,
)
from src.load import load_all, load_all_staged, load_star_schema

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
    """
    Execute the three-phase mapping-sheet-driven ETL pipeline.

    Parameters
    ----------
    mapping_sheet_path : str | Path
        Path to the DataBridge mapping sheet (.xlsx).
    db_path : str | None
        Override DuckDB path.
    progress_callback : callable | None
        Optional callback(phase: str, message: str, pct: float)
        where pct is 0.0–1.0 for progress bar updates.

    Returns
    -------
    PipelineRunMeta
        Metadata about the pipeline run.
    """
    meta = PipelineRunMeta(
        run_id=datetime.now().strftime("%Y%m%d_%H%M%S"),
        started_at=datetime.now(),
    )

    def _progress(phase: str, msg: str, pct: float):
        meta.phase = phase
        meta.status = phase.lower().replace(" ", "_")
        if progress_callback:
            progress_callback(phase, msg, pct)
        log.info("  [%s] %s (%.0f%%)", phase, msg, pct * 100)

    try:
        # ────────────────────────────────────────────────
        # Phase 1: EXTRACT — PostgreSQL → Parquet
        # ────────────────────────────────────────────────
        _progress("Extract", "Parsing mapping sheet…", 0.05)
        mapping = parse_mapping_sheet(mapping_sheet_path)
        source_tables = get_required_source_tables(mapping)

        _progress("Extract", f"Connecting to PostgreSQL…", 0.10)
        engine = get_pg_engine()

        _progress("Extract", f"Extracting {len(source_tables)} tables to Parquet…", 0.15)

        parquet_dir = STAGING_DIR
        manifest = extract_tables_to_parquet(
            engine, source_tables, parquet_dir,
            progress_callback=lambda name, idx, total: _progress(
                "Extract",
                f"Extracted {name} ({idx}/{total})",
                0.15 + (0.25 * idx / total),
            ),
        )

        meta.tables_extracted = list(manifest.keys())
        meta.parquet_files = [str(p) for p in manifest.values()]

        # ────────────────────────────────────────────────
        # Phase 2: TRANSFORM — Parquet → Star Schema
        # ────────────────────────────────────────────────
        _progress("Transform", "Building star-schema tables…", 0.45)

        result_dfs = run_mapping_transforms(
            parquet_dir, mapping,
            progress_callback=lambda name, step, total: _progress(
                "Transform",
                name,
                0.45 + (0.25 * step / total),
            ),
        )

        dim_df = result_dfs["dim_account_customer"]
        fact_df = result_dfs["fact_transactions"]
        meta.dim_row_count = len(dim_df)
        meta.fact_row_count = len(fact_df)

        # ────────────────────────────────────────────────
        # Phase 3: LOAD — DataFrames → DuckDB
        # ────────────────────────────────────────────────
        _progress("Load", "Loading star schema into DuckDB…", 0.75)

        load_star_schema(
            dim_df, fact_df, db_path,
            progress_callback=lambda name, step, total: _progress(
                "Load",
                name,
                0.75 + (0.20 * step / total),
            ),
        )

        meta.status = "done"
        meta.finished_at = datetime.now()
        _progress("Done", "Pipeline complete!", 1.0)
        log.info(
            "Pipeline finished in %.1fs — dim: %d rows, fact: %d rows",
            meta.duration_seconds, meta.dim_row_count, meta.fact_row_count,
        )

    except Exception as exc:
        meta.status = "failed"
        meta.error_message = str(exc)
        meta.finished_at = datetime.now()
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
