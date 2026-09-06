"""Idempotent ingestion of raw CSV files into the Bronze object-store layer.

Design goals (all called out in the README / interview doc):
  * config via env only, no hardcoded paths,
  * schema *presence* validation before a file is accepted,
  * idempotency: each file is landed under a deterministic, content-addressed
    key `bronze/<source>/ingest_date=<YYYY-MM-DD>/<source>__<md5>.csv`. Re-running
    the same day with unchanged data is a no-op (object already exists), so the
    DAG can safely retry,
  * uniform metrics: files discovered / records processed / rejected / loaded.

This stage does NOT clean data — it preserves the raw feed verbatim. Only files
whose required columns are missing are rejected here.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
from pathlib import Path

import pandas as pd

from src.ingestion.schemas import SOURCE_SCHEMAS
from src.utils.config import get_settings
from src.utils.io import RunMetrics, object_exists, put_bytes, s3_client
from src.utils.logging_config import get_logger

log = get_logger("ingestion.ingest")


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()[:12]


def discover_files(raw_dir: str) -> dict[str, Path]:
    """Map known source name -> csv path, for files that exist in raw_dir."""
    root = Path(raw_dir)
    found: dict[str, Path] = {}
    for name in SOURCE_SCHEMAS:
        p = root / f"{name}.csv"
        if p.exists():
            found[name] = p
    return found


def validate_columns(name: str, df: pd.DataFrame) -> list[str]:
    """Return the list of missing required columns (empty => ok)."""
    required = set(SOURCE_SCHEMAS[name].required_columns)
    return sorted(required - set(df.columns))


def ingest_file(client, source: str, path: Path, ingest_date: str,
                metrics: RunMetrics) -> bool:
    raw = path.read_bytes()
    df = pd.read_csv(path)
    missing = validate_columns(source, df)
    if missing:
        log.error("REJECT file %s: missing required columns %s", path.name, missing)
        metrics.records_rejected += len(df)
        return False

    key = f"{source}/ingest_date={ingest_date}/{source}__{_md5(raw)}.csv"
    settings = get_settings()
    if object_exists(client, settings.bucket_bronze, key):
        log.info("SKIP %s already landed (idempotent) -> %s", source, key)
        metrics.records_processed += len(df)
        return True

    put_bytes(client, settings.bucket_bronze, key, raw)
    # Also refresh a stable 'latest' pointer so downstream Spark reads a fixed
    # path (s3a://bronze/latest/<source>.csv) regardless of ingest_date.
    put_bytes(client, settings.bucket_bronze, f"latest/{source}.csv", raw)
    metrics.records_processed += len(df)
    metrics.records_loaded += len(df)
    log.info("LOADED %-12s %7d rows -> s3://%s/%s",
             source, len(df), settings.bucket_bronze, key)
    return True


def run(raw_dir: str | None = None, ingest_date: str | None = None) -> RunMetrics:
    settings = get_settings()
    raw_dir = raw_dir or settings.raw_data_dir
    ingest_date = ingest_date or dt.date.today().isoformat()
    metrics = RunMetrics(stage="ingestion")

    client = s3_client()
    files = discover_files(raw_dir)
    metrics.files_discovered = len(files)
    log.info("discovered %d source files in %s: %s",
             len(files), raw_dir, ", ".join(files) or "(none)")

    if not files:
        log.warning("no source files found in %s", raw_dir)

    for source, path in files.items():
        ingest_file(client, source, path, ingest_date, metrics)

    log.info(metrics.summary())
    return metrics


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest raw CSVs into Bronze (MinIO)")
    ap.add_argument("--raw-dir", default=None)
    ap.add_argument("--ingest-date", default=None, help="YYYY-MM-DD (default: today)")
    args = ap.parse_args()
    run(args.raw_dir, args.ingest_date)


if __name__ == "__main__":
    main()
