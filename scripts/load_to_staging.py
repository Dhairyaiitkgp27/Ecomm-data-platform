"""
Load the cleaned Silver layer into PostgreSQL `staging`.

This is a lightweight, Spark-free loader (pandas + SQLAlchemy). The production
path uses the Spark JDBC job (spark/jobs/silver_to_warehouse.py); this script is
a convenience for:
  * local runs / CI where standing up a Spark JDBC driver isn't worth it,
  * quickly (re)seeding staging so `dbt build` can run.

It reads the Silver Parquet produced by spark/jobs/bronze_to_silver.py and writes
one table per entity into the `staging` schema. Connection is env-driven.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ENTITIES = ["customers", "sellers", "products", "orders",
            "order_items", "payments", "shipments", "returns"]


def _engine():
    user = os.environ.get("POSTGRES_USER", "warehouse")
    pw = os.environ.get("POSTGRES_PASSWORD", "warehouse")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ecommerce")
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")


def _read_entity(base: Path, name: str) -> pd.DataFrame:
    """Read a Silver entity (Parquet dir or file), dropping Spark partition dirs
    from the column set if present."""
    path = base / name
    if not path.exists():
        # fall back to a flat parquet or csv file
        for cand in (base / f"{name}.parquet", base / f"{name}.csv"):
            if cand.exists():
                return pd.read_parquet(cand) if cand.suffix == ".parquet" else pd.read_csv(cand)
        raise FileNotFoundError(f"no Silver data for {name} under {base}")
    return pd.read_parquet(path)  # pyarrow reads a partitioned directory dataset


def run(silver_dir: str, schema: str = "staging") -> None:
    base = Path(silver_dir)
    eng = _engine()
    with eng.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    for name in ENTITIES:
        df = _read_entity(base, name)
        df.to_sql(name, eng, schema=schema, if_exists="replace",
                  index=False, chunksize=10_000, method="multi")
        print(f"[load] {schema}.{name:<12} <- {len(df):>8,} rows")


def main() -> None:
    ap = argparse.ArgumentParser(description="Load Silver Parquet into Postgres staging")
    ap.add_argument("--silver", default="data/silver")
    ap.add_argument("--schema", default="staging")
    args = ap.parse_args()
    run(args.silver, args.schema)


if __name__ == "__main__":
    main()
