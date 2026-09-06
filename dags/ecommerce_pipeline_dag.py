"""
Airflow DAG: end-to-end batch pipeline for the e-commerce warehouse.

Flow (linear, easy to explain in an interview):

    ingest_to_bronze          land raw CSVs into MinIO Bronze (idempotent)
        -> quality_gate       reject/quarantine/allow gate; fails fast on breach
        -> spark_bronze_to_silver   PySpark clean/dedup/standardise -> Silver
        -> spark_silver_to_warehouse  load Silver Parquet -> PostgreSQL staging
        -> dbt_snapshot        capture SCD2 seller versions
        -> dbt_run             build dims, facts and analytics marts
        -> dbt_test            run all dbt data tests
        -> warehouse_quality_check  final sanity assertions on the marts

Design choices:
  * retries + exponential-ish retry_delay so transient failures self-heal,
  * Spark/dbt run via BashOperator (spark-submit / dbt CLI) — mirrors what you
    run locally; Python steps use PythonOperator calling the src/ functions,
  * scheduled daily; catchup disabled so a paused DAG doesn't back-fill a storm,
  * no task explosion — one task per logical stage.

Incremental switch: set Airflow Variable `pipeline_mode = incremental` (or leave
`full`). Incremental mode passes --incremental to the Spark jobs so only orders
newer than the stored watermark are processed.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# --------------------------------------------------------------------------- #
# Paths / config (all env-driven; PROJECT_ROOT is mounted into the containers)
# --------------------------------------------------------------------------- #
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/project")
DBT_DIR = os.path.join(PROJECT_ROOT, "dbt")
BRONZE_INPUT = os.environ.get("SPARK_INPUT", "s3a://bronze/latest")
SILVER_OUTPUT = os.environ.get("SPARK_SILVER", "s3a://silver")
SPARK_PACKAGES = os.environ.get(
    "SPARK_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3",
)

try:
    PIPELINE_MODE = Variable.get("pipeline_mode", default_var="full")
except Exception:
    PIPELINE_MODE = "full"
INCREMENTAL_FLAG = "--incremental" if PIPELINE_MODE == "incremental" else ""

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=1),
}


def _ingest(**_):
    from src.ingestion.ingest import run
    m = run()
    print(m.summary())


def _quality_gate(**_):
    from src.quality.run_quality import run
    ok = run()
    if not ok:
        raise ValueError("Data quality gate FAILED — see report above. Stopping pipeline.")


def _warehouse_quality_check(**_):
    """A handful of post-build assertions straight against the warehouse."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
        user=os.environ.get("POSTGRES_USER", "warehouse"),
        password=os.environ.get("POSTGRES_PASSWORD", "warehouse"),
    )
    checks = {
        "fact_orders row count > 0":
            "select count(*) from marts.fact_orders",
        "no null order_id in fact_orders":
            "select count(*) from marts.fact_orders where order_id is null",
        "no negative gmv in daily_sales":
            "select count(*) from analytics.daily_sales where gmv_inr < 0",
    }
    with conn.cursor() as cur:
        cur.execute(checks["fact_orders row count > 0"]); 
        assert cur.fetchone()[0] > 0, "fact_orders is empty"
        cur.execute(checks["no null order_id in fact_orders"]); 
        assert cur.fetchone()[0] == 0, "null order_id found"
        cur.execute(checks["no negative gmv in daily_sales"]); 
        assert cur.fetchone()[0] == 0, "negative gmv found"
    conn.close()
    print("warehouse quality checks passed")


with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="Batch ELT: Bronze -> Silver -> Warehouse -> dbt marts",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",        # daily at 02:00
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "spark", "dbt"],
) as dag:

    ingest_to_bronze = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=_ingest,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=_quality_gate,
    )

    spark_bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=(
            f"spark-submit --master local[*] --packages {SPARK_PACKAGES} "
            f"{PROJECT_ROOT}/spark/jobs/bronze_to_silver.py "
            f"--input {BRONZE_INPUT} --output {SILVER_OUTPUT} {INCREMENTAL_FLAG}"
        ),
    )

    spark_silver_to_warehouse = BashOperator(
        task_id="spark_silver_to_warehouse",
        bash_command=(
            f"spark-submit --master local[*] --packages {SPARK_PACKAGES} "
            f"{PROJECT_ROOT}/spark/jobs/silver_to_warehouse.py "
            f"--silver {SILVER_OUTPUT} {INCREMENTAL_FLAG}"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir {DBT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    warehouse_quality_check = PythonOperator(
        task_id="warehouse_quality_check",
        python_callable=_warehouse_quality_check,
    )

    (
        ingest_to_bronze
        >> quality_gate
        >> spark_bronze_to_silver
        >> spark_silver_to_warehouse
        >> dbt_snapshot
        >> dbt_run
        >> dbt_test
        >> warehouse_quality_check
    )
