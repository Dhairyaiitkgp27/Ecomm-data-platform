"""The data-quality gate that Airflow runs between ingestion and Spark.

It loads the raw tables, runs the declarative suite, writes quarantined rows to
the quarantine location, prints a readable report, and decides pass/fail. When
DQ_FAIL_FAST is enabled and any severity exceeds its threshold, this exits
non-zero so the DAG task fails and the pipeline stops before polluting Silver.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from src.quality.rules import FAIL_THRESHOLDS, build_suite
from src.quality.validators import CheckResult, Severity
from src.ingestion.schemas import SOURCE_SCHEMAS
from src.utils.config import get_settings
from src.utils.logging_config import get_logger

log = get_logger("quality.gate")


def load_tables(raw_dir: str) -> dict[str, pd.DataFrame]:
    root = Path(raw_dir)
    out: dict[str, pd.DataFrame] = {}
    for name in SOURCE_SCHEMAS:
        p = root / f"{name}.csv"
        if p.exists():
            out[name] = pd.read_csv(p)
    return out


def write_quarantine(tables: dict[str, pd.DataFrame], results: list[CheckResult],
                     out_dir: str) -> int:
    """Collect quarantined rows per table and write them out for review."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    total = 0
    by_table: dict[str, pd.Series] = {}
    for r in results:
        if r.severity is Severity.QUARANTINE and r.mask is not None and r.n_violations:
            m = by_table.get(r.table)
            by_table[r.table] = r.mask if m is None else (m | r.mask)
    for table, mask in by_table.items():
        rows = tables[table][mask]
        if len(rows):
            path = out / f"{table}_quarantine.csv"
            rows.to_csv(path, index=False)
            total += len(rows)
            log.warning("quarantined %d rows from %s -> %s", len(rows), table, path)
    return total


def evaluate(results: list[CheckResult]) -> tuple[bool, list[str]]:
    breaches: list[str] = []
    for r in results:
        limit = FAIL_THRESHOLDS[r.severity]
        if r.violation_rate > limit and r.severity in (Severity.REJECT, Severity.QUARANTINE):
            breaches.append(f"{r.table}.{r.check} {r.violation_rate:.2%} > {limit:.0%}")
    return (len(breaches) == 0), breaches


def run(raw_dir: str | None = None, quarantine_dir: str = "data/quarantine",
        fail_fast: bool | None = None) -> bool:
    settings = get_settings()
    raw_dir = raw_dir or settings.raw_data_dir
    fail_fast = settings.dq_fail_fast if fail_fast is None else fail_fast

    tables = load_tables(raw_dir)
    if not tables:
        log.error("no raw tables found in %s", raw_dir)
        return False

    results = build_suite(tables)

    log.info("================= DATA QUALITY REPORT =================")
    for sev in (Severity.REJECT, Severity.QUARANTINE, Severity.CORRECT, Severity.ALLOW):
        section = [r for r in results if r.severity is sev]
        failed = [r for r in section if not r.passed]
        log.info("--- %s (%d checks, %d with findings) ---",
                 sev.value.upper(), len(section), len(failed))
        for r in failed:
            log.info("  %s", r)

    n_quarantined = write_quarantine(tables, results, quarantine_dir)
    ok, breaches = evaluate(results)

    log.info("======================================================")
    log.info("quarantined rows: %d", n_quarantined)
    if breaches:
        for b in breaches:
            log.error("THRESHOLD BREACH: %s", b)
    log.info("data quality gate: %s", "PASS" if ok else "FAIL")

    if not ok and fail_fast:
        return False
    return True


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the data-quality gate")
    ap.add_argument("--raw-dir", default=None)
    ap.add_argument("--quarantine-dir", default="data/quarantine")
    ap.add_argument("--no-fail-fast", action="store_true", help="report only, never exit non-zero")
    args = ap.parse_args()
    ok = run(args.raw_dir, args.quarantine_dir, fail_fast=not args.no_fail_fast)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
