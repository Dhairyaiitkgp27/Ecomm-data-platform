"""Shared Spark helpers: session builder, schemas and cleaning reference maps.

Kept separate so the Bronze->Silver and Silver->Warehouse jobs share one
SparkSession configuration (including the S3A/MinIO settings) and one source of
truth for how dirty values are canonicalised. The cleaning maps here must cover
every defect the generator injects (dirty cities, categories, payment statuses).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, DateType,
)


# --------------------------------------------------------------------------- #
# Spark session (local by default; S3A configured for MinIO)
# --------------------------------------------------------------------------- #
def get_spark(app_name: str = "ecommerce-transform") -> SparkSession:
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    access = os.environ.get("S3_ACCESS_KEY", "minioadmin")
    secret = os.environ.get("S3_SECRET_KEY", "minioadmin")

    builder = (
        SparkSession.builder.appName(app_name)
        # Sensible local defaults; overridden by spark-submit / cluster config.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        # S3A -> MinIO
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )
    return builder.getOrCreate()


# --------------------------------------------------------------------------- #
# Explicit read schemas (raw is read as strings where dirty, cast later)
# --------------------------------------------------------------------------- #
RAW_SCHEMAS: dict[str, StructType] = {
    "customers": StructType([
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("pincode", StringType()),
        StructField("signup_date", StringType()),
        StructField("customer_segment", StringType()),
    ]),
    "sellers": StructType([
        StructField("seller_id", StringType()),
        StructField("seller_name", StringType()),
        StructField("seller_city", StringType()),
        StructField("seller_state", StringType()),
        StructField("seller_tier", IntegerType()),
        StructField("seller_pincode", StringType()),
        StructField("onboarded_date", StringType()),
        StructField("seller_rating", DoubleType()),
        StructField("business_type", StringType()),
    ]),
    "products": StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("subcategory", StringType()),
        StructField("brand", StringType()),
        StructField("mrp_inr", DoubleType()),
        StructField("weight_grams", IntegerType()),
    ]),
    "orders": StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_ts", StringType()),
        StructField("order_approved_ts", StringType()),
        StructField("payment_type", StringType()),
    ]),
    "order_items": StructType([
        StructField("order_id", StringType()),
        StructField("order_item_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price_inr", DoubleType()),
        StructField("discount_inr", DoubleType()),
        StructField("freight_inr", DoubleType()),
        StructField("line_total_inr", DoubleType()),
    ]),
    "payments": StructType([
        StructField("order_id", StringType()),
        StructField("payment_sequential", IntegerType()),
        StructField("payment_type", StringType()),
        StructField("payment_installments", IntegerType()),
        StructField("payment_value_inr", DoubleType()),
        StructField("payment_status", StringType()),
    ]),
    "shipments": StructType([
        StructField("order_id", StringType()),
        StructField("carrier", StringType()),
        StructField("ship_ts", StringType()),
        StructField("estimated_delivery_date", StringType()),
        StructField("delivered_ts", StringType()),
        StructField("delivery_status", StringType()),
    ]),
    "returns": StructType([
        StructField("return_id", StringType()),
        StructField("order_id", StringType()),
        StructField("order_item_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("return_reason", StringType()),
        StructField("return_status", StringType()),
        StructField("return_qty", IntegerType()),
        StructField("refund_amount_inr", DoubleType()),
        StructField("return_requested_ts", StringType()),
        StructField("return_completed_ts", StringType()),
    ]),
}


# --------------------------------------------------------------------------- #
# Cleaning reference maps (alias/dirty -> canonical)
# --------------------------------------------------------------------------- #
CITY_CANONICAL = {
    "bangalore": "Bengaluru", "bengaluru": "Bengaluru",
    "bombay": "Mumbai", "mumbai": "Mumbai",
    "new delhi": "Delhi", "delhi": "Delhi",
    "calcutta": "Kolkata", "kolkata": "Kolkata",
    "madras": "Chennai", "chennai": "Chennai",
    "pondicherry": "Puducherry", "puducherry": "Puducherry",
    "trivandrum": "Thiruvananthapuram", "thiruvananthapuram": "Thiruvananthapuram",
    "mysore": "Mysuru", "mysuru": "Mysuru",
    "mangalore": "Mangaluru", "mangaluru": "Mangaluru",
    "baroda": "Vadodara", "vadodara": "Vadodara",
}

PAYMENT_STATUS_CANONICAL = {
    "paid": "paid", "success": "paid", "completed": "paid", "captured": "paid",
    "pending": "pending", "in_process": "pending", "processing": "pending",
    "failed": "failed", "declined": "failed",
    "refunded": "refunded", "refund": "refunded", "reversed": "refunded",
}


def _map_expr(colname: str, mapping: dict[str, str], default_col: bool = True):
    """Build a CASE WHEN chain that maps trimmed/lower values via `mapping`.
    Falls back to the trimmed original (title-cased) when no key matches."""
    key = F.lower(F.trim(F.col(colname)))
    expr = None
    for k, v in mapping.items():
        cond = key == k
        expr = F.when(cond, F.lit(v)) if expr is None else expr.when(cond, F.lit(v))
    fallback = F.initcap(F.trim(F.col(colname))) if default_col else F.lit(None)
    return expr.otherwise(fallback)


def standardize_city(df: DataFrame, col: str) -> DataFrame:
    return df.withColumn(col, _map_expr(col, CITY_CANONICAL))


def standardize_payment_status(df: DataFrame, col: str = "payment_status") -> DataFrame:
    key = F.lower(F.trim(F.col(col)))
    expr = None
    for k, v in PAYMENT_STATUS_CANONICAL.items():
        cond = key == k
        expr = F.when(cond, F.lit(v)) if expr is None else expr.when(cond, F.lit(v))
    return df.withColumn(col, expr.otherwise(F.lit("unknown")))


def canonicalize_category(df: DataFrame, col: str = "category") -> DataFrame:
    """Trim + collapse casing + normalise ' and ' vs ' & ' back to canonical."""
    cleaned = F.trim(F.regexp_replace(F.lower(F.col(col)), r"\s+and\s+", " & "))
    return df.withColumn(col, F.initcap(cleaned)) \
             .withColumn(col, F.regexp_replace(F.col(col), r"\bAnd\b", "&"))


def dedup_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep one row per key, the latest by order_col, using a window + row_number.
    This is how duplicate order/customer records are removed deterministically."""
    from pyspark.sql.window import Window
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))
