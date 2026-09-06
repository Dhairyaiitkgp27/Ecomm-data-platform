"""Pure, unit-testable data-quality checks.

Each function takes a DataFrame (and sometimes a reference set) and returns a
CheckResult describing what it found and how the platform will treat it. Keeping
these pure (no IO) is deliberate: they are trivial to unit test and are reused by
both the pre-Spark gate (src/quality/run_quality.py) and the test suite.

The four dispositions map directly to the documented DQ policy:
  REJECT      -> row is dropped and counted (e.g. null primary key)
  QUARANTINE  -> row is set aside for review, excluded from Silver (e.g. orphan FK)
  CORRECT     -> row is kept; Spark fixes it in Bronze->Silver (e.g. dirty city)
  ALLOW       -> row is kept as-is; recorded as a warning only (e.g. null review)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pandas as pd


class Severity(str, Enum):
    REJECT = "reject"
    QUARANTINE = "quarantine"
    CORRECT = "correct"
    ALLOW = "allow"


@dataclass
class CheckResult:
    check: str
    table: str
    severity: Severity
    n_violations: int
    n_total: int
    detail: str = ""
    # optional boolean mask (index-aligned) of the offending rows
    mask: pd.Series | None = None

    @property
    def violation_rate(self) -> float:
        return self.n_violations / self.n_total if self.n_total else 0.0

    @property
    def passed(self) -> bool:
        return self.n_violations == 0

    def __str__(self) -> str:
        return (f"{self.severity.value.upper():10} {self.table}.{self.check}: "
                f"{self.n_violations}/{self.n_total} ({self.violation_rate:.2%}) {self.detail}")


# --------------------------------------------------------------------------- #
# Individual checks
# --------------------------------------------------------------------------- #
def check_primary_key_unique(df: pd.DataFrame, table: str, keys: list[str]) -> CheckResult:
    mask = df.duplicated(subset=keys, keep=False)
    return CheckResult("pk_unique", table, Severity.REJECT, int(mask.sum()), len(df),
                       detail=f"keys={keys}", mask=mask)


def check_not_null(df: pd.DataFrame, table: str, cols: list[str]) -> CheckResult:
    present = [c for c in cols if c in df.columns]
    mask = df[present].isna().any(axis=1) if present else pd.Series(False, index=df.index)
    return CheckResult("critical_not_null", table, Severity.REJECT, int(mask.sum()), len(df),
                       detail=f"cols={present}", mask=mask)


def check_referential_integrity(df: pd.DataFrame, table: str, col: str,
                                parent_keys: set, ref_table: str) -> CheckResult:
    mask = ~df[col].isin(parent_keys) & df[col].notna()
    return CheckResult("referential_integrity", table, Severity.QUARANTINE,
                       int(mask.sum()), len(df),
                       detail=f"{col} not in {ref_table}", mask=mask)


def check_non_negative(df: pd.DataFrame, table: str, col: str,
                       allow_zero: bool = True) -> CheckResult:
    if col not in df.columns:
        return CheckResult(f"non_negative_{col}", table, Severity.REJECT, 0, len(df))
    vals = pd.to_numeric(df[col], errors="coerce")
    mask = (vals < 0) if allow_zero else (vals <= 0)
    sev = Severity.QUARANTINE if col.endswith("_inr") else Severity.REJECT
    return CheckResult(f"non_negative_{col}", table, sev, int(mask.fillna(False).sum()),
                       len(df), detail=f"allow_zero={allow_zero}", mask=mask.fillna(False))


def check_valid_values(df: pd.DataFrame, table: str, col: str,
                       allowed: set, case_insensitive: bool = True) -> CheckResult:
    """Values outside the accepted set. Marked CORRECT because Spark canonicalises
    casing/whitespace/aliases downstream rather than dropping the row."""
    if col not in df.columns:
        return CheckResult(f"valid_{col}", table, Severity.CORRECT, 0, len(df))
    s = df[col].astype("string")
    norm = s.str.strip().str.lower() if case_insensitive else s
    allowed_norm = {a.lower() for a in allowed} if case_insensitive else allowed
    mask = ~norm.isin(allowed_norm) & s.notna()
    return CheckResult(f"valid_{col}", table, Severity.CORRECT, int(mask.sum()), len(df),
                       detail=f"canonicalised to {sorted(allowed)[:4]}...", mask=mask)


def check_date_order(df: pd.DataFrame, table: str, earlier: str, later: str) -> CheckResult:
    """later should be >= earlier; violations are impossible timelines -> quarantine."""
    if earlier not in df.columns or later not in df.columns:
        return CheckResult(f"date_order_{later}_ge_{earlier}", table, Severity.QUARANTINE, 0, len(df))
    e = pd.to_datetime(df[earlier], errors="coerce")
    l = pd.to_datetime(df[later], errors="coerce")
    mask = (l < e) & e.notna() & l.notna()
    return CheckResult(f"date_order_{later}_ge_{earlier}", table, Severity.QUARANTINE,
                       int(mask.sum()), len(df), detail=f"{later} < {earlier}", mask=mask)


def check_future_dates(df: pd.DataFrame, table: str, col: str,
                       horizon: str = "2100-01-01") -> CheckResult:
    if col not in df.columns:
        return CheckResult(f"future_{col}", table, Severity.QUARANTINE, 0, len(df))
    d = pd.to_datetime(df[col], errors="coerce")
    mask = d > pd.Timestamp(horizon)
    return CheckResult(f"future_{col}", table, Severity.QUARANTINE,
                       int(mask.fillna(False).sum()), len(df), mask=mask.fillna(False))


def check_delivery_duration(df: pd.DataFrame, table: str,
                            start: str = "ship_ts", end: str = "delivered_ts",
                            max_days: int = 45) -> CheckResult:
    """Deliveries taking absurdly long are suspect -> allow but flag (soft check)."""
    if start not in df.columns or end not in df.columns:
        return CheckResult("delivery_duration_sane", table, Severity.ALLOW, 0, len(df))
    s = pd.to_datetime(df[start], errors="coerce")
    e = pd.to_datetime(df[end], errors="coerce")
    days = (e - s).dt.days
    mask = (days > max_days) & days.notna()
    return CheckResult("delivery_duration_sane", table, Severity.ALLOW,
                       int(mask.sum()), len(df), detail=f"> {max_days} days", mask=mask)
