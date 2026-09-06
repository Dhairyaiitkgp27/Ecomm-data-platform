"""Declared contracts for each raw source file.

These are *expectations about the raw feed*, used at ingestion time to decide
whether a file is even loadable (right columns present) and to record a schema
fingerprint. They are intentionally permissive on types because the raw layer
is meant to hold messy data as-is — real type enforcement happens in Spark
(Bronze -> Silver). `required_columns` must all be present or the file is
rejected wholesale; `primary_key` and `not_null` inform the quality layer.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceSchema:
    name: str
    required_columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    not_null: tuple[str, ...]           # critical fields; null => reject row
    foreign_keys: tuple[tuple[str, str, str], ...] = ()  # (col, ref_table, ref_col)


SOURCE_SCHEMAS: dict[str, SourceSchema] = {
    "customers": SourceSchema(
        name="customers",
        required_columns=("customer_id", "customer_name", "city", "state",
                          "pincode", "signup_date", "customer_segment"),
        primary_key=("customer_id",),
        not_null=("customer_id",),
    ),
    "sellers": SourceSchema(
        name="sellers",
        required_columns=("seller_id", "seller_name", "seller_city", "seller_state",
                          "seller_tier", "onboarded_date", "seller_rating", "business_type"),
        primary_key=("seller_id",),
        not_null=("seller_id",),
    ),
    "products": SourceSchema(
        name="products",
        required_columns=("product_id", "product_name", "category", "subcategory",
                          "brand", "mrp_inr", "weight_grams"),
        primary_key=("product_id",),
        not_null=("product_id",),
    ),
    "orders": SourceSchema(
        name="orders",
        required_columns=("order_id", "customer_id", "order_status",
                          "order_purchase_ts", "order_approved_ts", "payment_type"),
        primary_key=("order_id",),
        not_null=("order_id", "customer_id", "order_purchase_ts"),
        foreign_keys=(("customer_id", "customers", "customer_id"),),
    ),
    "order_items": SourceSchema(
        name="order_items",
        required_columns=("order_id", "order_item_id", "product_id", "seller_id",
                          "quantity", "unit_price_inr", "discount_inr", "freight_inr"),
        primary_key=("order_id", "order_item_id"),
        not_null=("order_id", "order_item_id", "product_id"),
        foreign_keys=(("order_id", "orders", "order_id"),
                      ("product_id", "products", "product_id"),
                      ("seller_id", "sellers", "seller_id")),
    ),
    "payments": SourceSchema(
        name="payments",
        required_columns=("order_id", "payment_sequential", "payment_type",
                          "payment_installments", "payment_value_inr", "payment_status"),
        primary_key=("order_id", "payment_sequential"),
        not_null=("order_id", "payment_sequential"),
        foreign_keys=(("order_id", "orders", "order_id"),),
    ),
    "shipments": SourceSchema(
        name="shipments",
        required_columns=("order_id", "carrier", "ship_ts",
                          "estimated_delivery_date", "delivered_ts", "delivery_status"),
        primary_key=("order_id",),
        not_null=("order_id",),
        foreign_keys=(("order_id", "orders", "order_id"),),
    ),
    "returns": SourceSchema(
        name="returns",
        required_columns=("return_id", "order_id", "order_item_id", "product_id",
                          "return_reason", "return_status", "return_qty",
                          "refund_amount_inr", "return_requested_ts", "return_completed_ts"),
        primary_key=("return_id",),
        not_null=("return_id", "order_id"),
        foreign_keys=(("order_id", "orders", "order_id"),),
    ),
}
