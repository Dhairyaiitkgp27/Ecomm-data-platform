"""Tests for the synthetic data generator: structure, integrity, and defects."""
from __future__ import annotations

from src.ingestion.schemas import SOURCE_SCHEMAS


def test_all_tables_present(small_dataset):
    assert set(small_dataset) == set(SOURCE_SCHEMAS)


def test_required_columns_present(small_dataset):
    for name, schema in SOURCE_SCHEMAS.items():
        cols = set(small_dataset[name].columns)
        assert set(schema.required_columns).issubset(cols), f"{name} missing columns"


def test_orders_have_items(small_dataset):
    orders = set(small_dataset["orders"]["order_id"])
    items_orders = set(small_dataset["order_items"]["order_id"])
    # Most orders should have at least one item (orphans are a small injected defect).
    overlap = len(orders & items_orders) / len(orders)
    assert overlap > 0.9


def test_defects_are_injected(small_dataset):
    orders = small_dataset["orders"]
    customers = small_dataset["customers"]
    order_items = small_dataset["order_items"]
    # duplicates exist
    assert orders["order_id"].duplicated().any()
    # some dirty/absent cities exist
    assert customers["city"].isna().any() or \
        customers["city"].astype(str).str.contains("Bangalore|Bombay", case=False).any()
    # some non-positive quantities exist
    assert (order_items["quantity"] <= 0).any()


def test_price_bands_are_positive_mostly(small_dataset):
    prod = small_dataset["products"]
    # negative prices are a rare injected defect; the vast majority are positive
    assert (prod["mrp_inr"] > 0).mean() > 0.95


def test_cod_share_is_realistic(small_dataset):
    share = (small_dataset["orders"]["payment_type"] == "COD").mean()
    assert 0.4 < share < 0.7
