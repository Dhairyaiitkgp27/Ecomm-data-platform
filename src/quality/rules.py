"""Declarative data-quality suite + fail thresholds.

`build_suite` returns the list of checks to run against each raw table given the
loaded reference sets. `FAIL_THRESHOLDS` define the maximum tolerable violation
rate per severity before the pipeline gate fails (when DQ_FAIL_FAST is on).

Policy summary (also in docs/data_quality.md):
  REJECT      duplicate PKs, null critical fields
  QUARANTINE  orphan FKs, impossible/absurd dates, negative money amounts
  CORRECT     dirty city names, malformed categories, messy payment statuses
  ALLOW       null non-critical fields, unusually long deliveries
"""

from __future__ import annotations

import pandas as pd

from src.quality import validators as V
from src.quality.validators import CheckResult, Severity

# Canonical accepted value sets (Spark canonicalises to these).
VALID_ORDER_STATUS = {"delivered", "shipped", "processing", "cancelled", "returned"}
VALID_PAYMENT_STATUS = {"paid", "pending", "failed", "refunded"}
VALID_CATEGORIES = {
    "mobiles & accessories", "electronics", "fashion", "home & kitchen",
    "beauty & personal care", "appliances", "books", "grocery",
    "toys & baby", "sports & fitness",
}

# Max tolerable violation rate before the gate fails, by severity.
FAIL_THRESHOLDS = {
    Severity.REJECT: 0.05,       # >5% unusable rows in a table => stop
    Severity.QUARANTINE: 0.10,   # >10% quarantined => stop and investigate
    Severity.CORRECT: 1.00,      # corrections never fail the pipeline
    Severity.ALLOW: 1.00,        # allowed issues never fail the pipeline
}


def build_suite(tables: dict[str, pd.DataFrame]) -> list[CheckResult]:
    results: list[CheckResult] = []

    customers = tables.get("customers")
    orders = tables.get("orders")
    order_items = tables.get("order_items")
    payments = tables.get("payments")
    products = tables.get("products")
    sellers = tables.get("sellers")
    shipments = tables.get("shipments")
    returns = tables.get("returns")

    cust_keys = set(customers["customer_id"]) if customers is not None else set()
    order_keys = set(orders["order_id"]) if orders is not None else set()
    prod_keys = set(products["product_id"]) if products is not None else set()
    sell_keys = set(sellers["seller_id"]) if sellers is not None else set()

    if customers is not None:
        results += [
            V.check_primary_key_unique(customers, "customers", ["customer_id"]),
            V.check_not_null(customers, "customers", ["customer_id"]),
        ]
    if sellers is not None:
        results += [
            V.check_primary_key_unique(sellers, "sellers", ["seller_id"]),
            V.check_not_null(sellers, "sellers", ["seller_id"]),
        ]
    if products is not None:
        results += [
            V.check_primary_key_unique(products, "products", ["product_id"]),
            V.check_not_null(products, "products", ["product_id"]),
            V.check_non_negative(products, "products", "mrp_inr", allow_zero=False),
            V.check_valid_values(products, "products", "category", VALID_CATEGORIES),
        ]
    if orders is not None:
        results += [
            V.check_primary_key_unique(orders, "orders", ["order_id"]),
            V.check_not_null(orders, "orders", ["order_id", "customer_id", "order_purchase_ts"]),
            V.check_referential_integrity(orders, "orders", "customer_id", cust_keys, "customers"),
            V.check_valid_values(orders, "orders", "order_status", VALID_ORDER_STATUS),
        ]
    if order_items is not None:
        results += [
            V.check_primary_key_unique(order_items, "order_items", ["order_id", "order_item_id"]),
            V.check_not_null(order_items, "order_items", ["order_id", "order_item_id", "product_id"]),
            V.check_referential_integrity(order_items, "order_items", "order_id", order_keys, "orders"),
            V.check_referential_integrity(order_items, "order_items", "product_id", prod_keys, "products"),
            V.check_referential_integrity(order_items, "order_items", "seller_id", sell_keys, "sellers"),
            V.check_non_negative(order_items, "order_items", "quantity", allow_zero=False),
            V.check_non_negative(order_items, "order_items", "unit_price_inr", allow_zero=True),
        ]
    if payments is not None:
        results += [
            V.check_primary_key_unique(payments, "payments", ["order_id", "payment_sequential"]),
            V.check_referential_integrity(payments, "payments", "order_id", order_keys, "orders"),
            V.check_valid_values(payments, "payments", "payment_status", VALID_PAYMENT_STATUS),
        ]
    if shipments is not None:
        results += [
            V.check_referential_integrity(shipments, "shipments", "order_id", order_keys, "orders"),
            V.check_date_order(shipments, "shipments", "ship_ts", "delivered_ts"),
            V.check_future_dates(shipments, "shipments", "delivered_ts"),
            V.check_delivery_duration(shipments, "shipments"),
        ]
    if returns is not None:
        results += [
            V.check_primary_key_unique(returns, "returns", ["return_id"]),
            V.check_referential_integrity(returns, "returns", "order_id", order_keys, "orders"),
        ]
    return results
