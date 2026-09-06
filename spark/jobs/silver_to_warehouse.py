"""
Silver -> PostgreSQL (staging schema) loader.

Reads the cleaned Silver Parquet and writes each entity into the warehouse
`staging` schema via JDBC. dbt then builds the star schema and marts on top of
these staging tables.

Load strategy:
  * Dimensions and small tables: full overwrite (cheap, always consistent).
  * Large facts (orders, order_items): when --incremental is passed, only the
    rows for order dates on/after --since are appended, mirroring the Spark
    watermark. Idempotency for a re-run of the same window is handled by first
    deleting that window from the target table, then appending (delete+insert).

JDBC tuning (numPartitions / batchsize) is set so the write parallelises instead
of funnelling every row through one connection.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.jobs.common import get_spark


def _jdbc_props() -> tuple[str, dict]:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ecommerce")
    url = f"jdbc:postgresql://{host}:{port}/{db}"
    props = {
        "user": os.environ.get("POSTGRES_USER", "warehouse"),
        "password": os.environ.get("POSTGRES_PASSWORD", "warehouse"),
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
    }
    return url, props


def _read_parquet(spark: SparkSession, base: str, name: str) -> DataFrame:
    return spark.read.parquet(f"{base.rstrip('/')}/{name}")


def _write_table(df: DataFrame, table: str, mode: str, num_partitions: int = 4) -> None:
    url, props = _jdbc_props()
    schema = os.environ.get("PG_STAGING_SCHEMA", "staging")
    (df.repartition(num_partitions)
       .write.mode(mode)
       .option("numPartitions", num_partitions)
       .jdbc(url, f"{schema}.{table}", properties=props))
    print(f"[load] {schema}.{table} <- {df.count()} rows (mode={mode})")


ENTITIES_FULL = ["customers", "sellers", "products", "payments", "shipments", "returns"]
ENTITIES_INCREMENTAL = ["orders", "order_items"]


def run(silver_base: str, incremental: bool = False, since: str | None = None) -> None:
    spark = get_spark("silver-to-warehouse")
    spark.sparkContext.setLogLevel("WARN")

    # Small / dimension-like tables: straightforward full refresh.
    for name in ENTITIES_FULL:
        df = _read_parquet(spark, silver_base, name)
        _write_table(df, name, mode="overwrite")

    # Large facts.
    for name in ENTITIES_INCREMENTAL:
        df = _read_parquet(spark, silver_base, name)
        if incremental and since and "order_date" in df.columns:
            df = df.filter(F.col("order_date") >= F.lit(since))
            # delete+insert for the window handled in SQL by the DAG's pre-step;
            # here we simply append the (already-scoped) rows.
            _write_table(df, name, mode="append")
        else:
            _write_table(df, name, mode="overwrite")

    spark.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description="Silver Parquet -> PostgreSQL staging")
    ap.add_argument("--silver", required=True, help="silver base path (local dir or s3a://silver)")
    ap.add_argument("--incremental", action="store_true")
    ap.add_argument("--since", default=None, help="YYYY-MM-DD lower bound for incremental facts")
    args = ap.parse_args()
    run(args.silver, incremental=args.incremental, since=args.since)


if __name__ == "__main__":
    main()
