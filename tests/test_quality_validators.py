"""Unit tests for the pure data-quality validators."""
from __future__ import annotations

import pandas as pd

from src.quality import validators as V
from src.quality.validators import Severity


def test_pk_unique_detects_dupes():
    df = pd.DataFrame({"id": [1, 2, 2, 3]})
    r = V.check_primary_key_unique(df, "t", ["id"])
    assert r.n_violations == 2
    assert r.severity is Severity.REJECT


def test_not_null_detects_missing():
    df = pd.DataFrame({"id": [1, None, 3], "x": [1, 2, 3]})
    r = V.check_not_null(df, "t", ["id"])
    assert r.n_violations == 1


def test_referential_integrity_flags_orphans():
    df = pd.DataFrame({"fk": ["a", "b", "z"]})
    r = V.check_referential_integrity(df, "t", "fk", {"a", "b"}, "parent")
    assert r.n_violations == 1
    assert r.severity is Severity.QUARANTINE


def test_non_negative_quantity_disallows_zero():
    df = pd.DataFrame({"quantity": [1, 0, -2, 5]})
    r = V.check_non_negative(df, "t", "quantity", allow_zero=False)
    assert r.n_violations == 2


def test_valid_values_is_case_insensitive_correct():
    df = pd.DataFrame({"status": ["PAID", "paid", "weird", None]})
    r = V.check_valid_values(df, "t", "status", {"paid"})
    assert r.severity is Severity.CORRECT
    assert r.n_violations == 1  # only 'weird' is outside the set


def test_date_order_flags_impossible_timeline():
    df = pd.DataFrame({
        "a": pd.to_datetime(["2024-01-10", "2024-01-10"]),
        "b": pd.to_datetime(["2024-01-12", "2024-01-05"]),
    })
    r = V.check_date_order(df, "t", "a", "b")
    assert r.n_violations == 1


def test_violation_rate_and_passed():
    df = pd.DataFrame({"id": [1, 1]})
    r = V.check_primary_key_unique(df, "t", ["id"])
    assert not r.passed
    assert r.violation_rate == 1.0
