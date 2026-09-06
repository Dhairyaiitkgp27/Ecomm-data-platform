"""
Bronze -> Silver transformation (the core PySpark job).

What it demonstrates (each is called out in docs/spark_optimizations.md):
  * explicit read schemas (no expensive inferSchema on dirty data),
  * deterministic de-duplication with a window + row_number,
  * canonicalisation of dirty cities / categories / payment statuses,
  * null handling and rejection of unusable rows (null PK, non-positive qty),
  * quarantine of orphan order_items and impossible-timeline shipments,
  * date parsing + derived business metrics (delivery days, is_late, is_cod),
  * a broadcast join (small product dim) to enrich the large order_items fact,
  * a window function (per-customer order sequence) used downstream for repeat
    vs new customer analysis,
  * partitioned Parquet output (by order year/month) for efficient reads,
  * an incremental mode driven by a stored high-water mark on order_purchase_ts.

Runs identically on local paths (for tests) and on s3a://bronze and s3a://silver buckets.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.jobs.common import (
    RAW_SCHEMAS, canonicalize_category, dedup_latest, get_spark,
    standardize_city, standardize_payment_status,
)

WATERMARK_FILE = os.environ.get("WATERMARK_FILE", "data/_watermark.json")


def _read(spark: SparkSession, base: str, source: str) -> DataFrame:
    """Read a raw source from `base` (local dir or s3a path) with explicit schema."""
    path = f"{base.rstrip('/')}/{source}.csv"
    return (spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(RAW_SCHEMAS[source])
            .csv(path))


def _write(df: DataFrame, out_base: str, name: str, partition_by: list[str] | None = None,
           mode: str = "overwrite") -> None:
    path = f"{out_base.rstrip('/')}/{name}"
    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


# --------------------------------------------------------------------------- #
# Watermark (incremental)
# --------------------------------------------------------------------------- #
def read_watermark() -> str | None:
    p = Path(WATERMARK_FILE)
    if p.exists():
        return json.loads(p.read_text()).get("orders_purchase_ts")
    return None


def write_watermark(value: str) -> None:
    p = Path(WATERMARK_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"orders_purchase_ts": value}))


# --------------------------------------------------------------------------- #
# Per-entity transforms
# --------------------------------------------------------------------------- #
def transform_customers(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("customer_id").isNotNull())
    df = dedup_latest(df, ["customer_id"], "signup_date")
    df = standardize_city(df, "city")
    df = df.withColumn("city", F.coalesce(F.col("city"), F.lit("Unknown"))) \
           .withColumn("state", F.initcap(F.trim(F.col("state")))) \
           .withColumn("state", F.coalesce(F.col("state"), F.lit("Unknown"))) \
           .withColumn("signup_date", F.to_date("signup_date"))
    return df


def transform_sellers(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("seller_id").isNotNull())
    df = dedup_latest(df, ["seller_id"], "onboarded_date")
    df = standardize_city(df, "seller_city") \
        .withColumn("seller_state", F.initcap(F.trim(F.col("seller_state")))) \
        .withColumn("onboarded_date", F.to_date("onboarded_date"))
    return df


def transform_products(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("product_id").isNotNull())
    df = dedup_latest(df, ["product_id"], "product_id")
    df = canonicalize_category(df, "category")
    # negative MRP is invalid -> null it out (kept, but measure is nulled)
    df = df.withColumn(
        "mrp_inr",
        F.when(F.col("mrp_inr") < 0, F.lit(None)).otherwise(F.col("mrp_inr")),
    )
    return df


def transform_orders(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
    df = dedup_latest(df, ["order_id"], "order_approved_ts")
    df = (df
          .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
          .withColumn("order_purchase_ts", F.to_timestamp("order_purchase_ts"))
          .withColumn("order_approved_ts", F.to_timestamp("order_approved_ts"))
          .withColumn("order_date", F.to_date("order_purchase_ts"))
          .withColumn("order_year", F.year("order_purchase_ts"))
          .withColumn("order_month", F.month("order_purchase_ts"))
          .withColumn("is_cod", (F.col("payment_type") == F.lit("COD")).cast("int")))

    # Window function: per-customer chronological order sequence (1 = first order)
    w = Window.partitionBy("customer_id").orderBy("order_purchase_ts")
    df = df.withColumn("customer_order_seq", F.row_number().over(w)) \
           .withColumn("is_repeat_order", (F.col("customer_order_seq") > 1).cast("int"))
    return df


def transform_order_items(items: DataFrame, valid_order_ids: DataFrame,
                          products: DataFrame) -> tuple[DataFrame, DataFrame]:
    items = items.filter(
        F.col("order_id").isNotNull()
        & F.col("order_item_id").isNotNull()
        & F.col("product_id").isNotNull()
    )
    items = dedup_latest(items, ["order_id", "order_item_id"], "order_item_id")
    # Non-positive quantity is unusable -> drop
    items = items.filter(F.col("quantity") > 0)

    # Quarantine orphans: order_id not present in the cleaned orders set.
    orphan = items.join(valid_order_ids, "order_id", "left_anti")
    clean = items.join(valid_order_ids, "order_id", "left_semi")

    # Broadcast join to enrich with product category (products dim is small).
    prod_small = products.select("product_id", "category", "subcategory", "brand")
    clean = clean.join(F.broadcast(prod_small), "product_id", "left")

    # Recompute line total robustly (raw one may be dirty / missing).
    clean = clean.withColumn(
        "line_total_inr",
        F.round(F.col("unit_price_inr") * F.col("quantity") + F.coalesce(F.col("freight_inr"), F.lit(0)), 2),
    )
    return clean, orphan


def transform_payments(df: DataFrame, valid_order_ids: DataFrame) -> DataFrame:
    df = dedup_latest(df, ["order_id", "payment_sequential"], "payment_sequential")
    df = standardize_payment_status(df, "payment_status")
    df = df.join(valid_order_ids, "order_id", "left_semi")
    # null payment value -> 0 with a flag column (kept, ALLOW policy)
    df = df.withColumn("payment_value_missing", F.col("payment_value_inr").isNull().cast("int")) \
           .withColumn("payment_value_inr", F.coalesce(F.col("payment_value_inr"), F.lit(0.0)))
    return df


def transform_shipments(df: DataFrame, valid_order_ids: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = (df
          .withColumn("ship_ts", F.to_timestamp("ship_ts"))
          .withColumn("delivered_ts", F.to_timestamp("delivered_ts"))
          .withColumn("estimated_delivery_date", F.to_date("estimated_delivery_date")))
    df = df.join(valid_order_ids, "order_id", "left_semi")

    # Quarantine impossible timelines (delivered before shipped) or absurd future dates.
    bad = (F.col("delivered_ts") < F.col("ship_ts")) | (F.col("delivered_ts") > F.lit("2099-01-01"))
    quarantine = df.filter(bad)
    clean = df.filter(~bad | F.col("delivered_ts").isNull())

    clean = (clean
             .withColumn("delivery_days",
                         F.datediff(F.to_date("delivered_ts"), F.to_date("ship_ts")))
             .withColumn("is_late",
                         (F.to_date("delivered_ts") > F.col("estimated_delivery_date")).cast("int")))
    return clean, quarantine


def transform_returns(df: DataFrame, valid_order_ids: DataFrame) -> DataFrame:
    df = dedup_latest(df, ["return_id"], "return_requested_ts")
    df = (df
          .withColumn("return_requested_ts", F.to_timestamp("return_requested_ts"))
          .withColumn("return_completed_ts", F.to_timestamp("return_completed_ts"))
          .withColumn("days_to_return_complete",
                      F.datediff(F.to_date("return_completed_ts"), F.to_date("return_requested_ts"))))
    df = df.join(valid_order_ids, "order_id", "left_semi")
    return df


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run(input_base: str, output_base: str, incremental: bool = False) -> None:
    spark = get_spark("bronze-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    orders = transform_orders(_read(spark, input_base, "orders"))

    if incremental:
        wm = read_watermark()
        if wm:
            before = orders.count()
            orders = orders.filter(F.col("order_purchase_ts") > F.lit(wm))
            print(f"[incremental] watermark={wm}: {before} -> {orders.count()} orders after filter")

    # orders is reused for several semi/anti joins and the max-watermark calc,
    # so caching avoids recomputing the (non-trivial) dedup + window each time.
    orders.cache()
    valid_order_ids = orders.select("order_id").distinct()

    customers = transform_customers(_read(spark, input_base, "customers"))
    sellers = transform_sellers(_read(spark, input_base, "sellers"))
    products = transform_products(_read(spark, input_base, "products"))

    items_clean, items_orphan = transform_order_items(
        _read(spark, input_base, "order_items"), valid_order_ids, products)
    payments = transform_payments(_read(spark, input_base, "payments"), valid_order_ids)
    ship_clean, ship_quarantine = transform_shipments(
        _read(spark, input_base, "shipments"), valid_order_ids)
    returns = transform_returns(_read(spark, input_base, "returns"), valid_order_ids)

    write_mode = "append" if incremental else "overwrite"

    # Partition the large, time-series facts by year/month; small dims unpartitioned.
    _write(orders, output_base, "orders", ["order_year", "order_month"], write_mode)
    _write(items_clean, output_base, "order_items", None, write_mode)
    _write(payments, output_base, "payments", None, write_mode)
    _write(ship_clean, output_base, "shipments", None, write_mode)
    _write(returns, output_base, "returns", None, write_mode)
    _write(customers, output_base, "customers", None, "overwrite")
    _write(sellers, output_base, "sellers", None, "overwrite")
    _write(products, output_base, "products", None, "overwrite")

    # Quarantine outputs (kept, never dropped silently).
    _write(items_orphan, f"{output_base}/_quarantine", "order_items_orphan", None, write_mode)
    _write(ship_quarantine, f"{output_base}/_quarantine", "shipments_bad_dates", None, write_mode)

    # Advance the high-water mark to the newest purchase timestamp we processed.
    new_wm = orders.agg(F.max("order_purchase_ts").alias("m")).collect()[0]["m"]
    if new_wm is not None:
        write_watermark(str(new_wm))
        print(f"[watermark] advanced to {new_wm}")

    orders.unpersist()
    spark.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description="Bronze -> Silver PySpark job")
    ap.add_argument("--input", required=True, help="raw base path (local dir or s3a://bronze)")
    ap.add_argument("--output", required=True, help="silver base path (local dir or s3a://silver)")
    ap.add_argument("--incremental", action="store_true",
                    help="process only orders newer than the stored watermark")
    args = ap.parse_args()
    run(args.input, args.output, incremental=args.incremental)


if __name__ == "__main__":
    main()
