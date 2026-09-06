# Architecture

## Overview

A batch **ELT** pipeline that turns eight messy source feeds into a clean,
tested analytical warehouse with a **medallion** (Bronze → Silver → Gold) layout
and a **star schema** at the Gold layer. Each tool is used for the one job it is
best at, with no overlap.

```
 ┌──────────────┐
 │   Sources    │  8 raw CSV feeds (synthetic):
 │              │  customers, sellers, products, orders,
 │              │  order_items, payments, shipments, returns
 └──────┬───────┘
        │  ingestion (Python, idempotent, content-addressed keys)
        ▼
 ┌──────────────┐   MinIO (S3-compatible object store)
 │ BRONZE (lake)│   raw, immutable, partitioned by ingest_date
 └──────┬───────┘   + a stable latest/ pointer for Spark
        │  data-quality GATE (Python): reject / quarantine / correct / allow
        │  ── fails fast if a table breaches thresholds ──
        ▼
 ┌──────────────┐   PySpark
 │ SILVER (lake)│   dedup, standardise cities/categories/statuses, null-handling,
 │              │   derived metrics, partitioned Parquet (order year/month)
 └──────┬───────┘
        │  load (PySpark JDBC)
        ▼
 ┌──────────────┐   PostgreSQL schema: staging
 │  STAGING(DW) │   cleaned tables, 1:1 with Silver
 └──────┬───────┘
        │  dbt (staging views → intermediate → marts)
        │  + dbt snapshot (SCD2 sellers)
        ▼
 ┌──────────────┐   PostgreSQL schemas: marts (dims + facts), analytics (marts)
 │  GOLD (DW)   │   star schema: dim_* + fact_* ; business marts ; dbt tests
 └──────┬───────┘
        │
        ▼
 ┌──────────────┐
 │  Streamlit   │   KPIs, trends, top sellers/products, delivery SLA
 └──────────────┘

 Orchestration:  Airflow DAG runs ingest → quality → spark×2 → dbt snapshot/run/test → checks
```

## Why each layer exists (division of labour)

| Layer | Tool | Responsibility | Why here and not elsewhere |
|---|---|---|---|
| Ingestion | Python + boto3 | Land raw feeds idempotently in Bronze | Lightweight; no transformation needed yet |
| Quality gate | Python (pandas) | Structural checks, quarantine, fail-fast | Cheap row-level checks before paying for Spark |
| Bronze→Silver | **PySpark** | Heavy row-level cleaning, dedup, standardisation, partitioning | Set-based, scales to large volumes; the "hard" distributed work |
| Silver→Staging | PySpark JDBC | Load cleaned Parquet into Postgres | One tool already in hand; parallel JDBC write |
| Staging→Gold | **dbt** | Set-based SQL modelling, star schema, SCD2, tests, docs, lineage | dbt is best at SQL transformations + testing + documentation |
| Serving | Streamlit | Visualise marts | Runnable and reviewable inside Docker |
| Orchestration | Airflow | Schedule, dependencies, retries, backfill control | Standard batch orchestrator |

**No overlap:** Spark does the imperative, row-level, distributed cleaning that
is painful in SQL (dedup windows across the whole feed, per-row standardisation,
partitioned file output). dbt does the declarative, set-based modelling that is
painful in Spark (joins into a star schema, incremental merges, tests, docs).

## Storage layout

- **Bronze** `s3a://bronze/<source>/ingest_date=YYYY-MM-DD/<source>__<md5>.csv`
  (+ `s3a://bronze/latest/<source>.csv`)
- **Silver** `s3a://silver/<entity>/…` Parquet; facts partitioned by
  `order_year`/`order_month`; a `_quarantine/` prefix for set-aside rows.
- **Warehouse** `staging.*` (Spark-loaded) → `marts.*` + `analytics.*` (dbt).

## Data flow guarantees

- **Idempotent ingestion** — content-addressed keys; re-running a day is a no-op.
- **Fail-fast quality** — the gate stops the pipeline before bad data reaches Silver.
- **Incremental** — Spark uses a high-water mark on `order_purchase_ts`; the
  `fact_order_items` and `daily_sales` dbt models are incremental.
- **Recoverability** — Airflow retries with backoff; each stage is re-runnable.
