"""Tests for ingestion schema-presence validation and config."""
from __future__ import annotations

import pandas as pd

from src.ingestion.ingest import validate_columns
from src.ingestion.schemas import SOURCE_SCHEMAS


def test_validate_columns_passes_on_full_schema():
    schema = SOURCE_SCHEMAS["customers"]
    df = pd.DataFrame(columns=list(schema.required_columns))
    assert validate_columns("customers", df) == []


def test_validate_columns_reports_missing():
    df = pd.DataFrame(columns=["customer_id"])  # missing many
    missing = validate_columns("customers", df)
    assert "customer_name" in missing
    assert "city" in missing
