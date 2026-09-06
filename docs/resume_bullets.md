# Resume bullets & project description

Use whichever fit your resume. **All claims are honest and demonstrable from this
repo** — no invented scale or fake employer. Numbers refer to what the project
actually contains/does (configurable ~200k orders; the committed sample is ~3k).

> Replace `<GitHub link>` and tailor wording to the role.

## One-line bullets (pick 2–3)

- Built an end-to-end **batch ELT pipeline** (Python → MinIO data lake → PySpark →
  PostgreSQL → dbt → Streamlit) for a synthetic Indian e-commerce dataset,
  orchestrated with **Airflow**.
- Designed a **star-schema warehouse** (5 fact tables, 6 dimensions) in dbt with
  **SCD Type 2** history, incremental models, and 40+ automated data tests.
- Implemented a **medallion (Bronze/Silver/Gold)** architecture with a
  reject/quarantine/correct/allow **data-quality framework** that fails the
  pipeline on threshold breaches.
- Engineered **PySpark** transformations using window-based deduplication,
  broadcast joins, partitioned Parquet, and high-water-mark **incremental**
  processing.

## Detailed bullets (STAR-ish, for the projects section)

**E-Commerce Data Engineering Platform** — *personal project* · `<GitHub link>`

- Architected a medallion-style **batch ELT** pipeline processing eight related
  source feeds (orders, items, payments, shipments, returns, customers, sellers,
  products) into a tested analytical warehouse; **Airflow** DAG runs
  ingest → quality gate → PySpark (Bronze→Silver) → JDBC load → dbt
  snapshot/run/test with retries and fail-fast quality.
- Built a **PySpark** cleaning layer handling real data-quality problems —
  deterministic **de-duplication** via `row_number()` windows, city/category/
  payment-status **standardisation**, orphan-record **quarantine**, and
  **partitioned Parquet** output; added **incremental** processing with a
  high-water mark to avoid reprocessing history.
- Modelled a **star schema** in **dbt** (fact_orders, fact_order_items,
  fact_payments, fact_shipments, fact_returns + conformed dimensions) with a
  **dbt snapshot for SCD Type 2** seller history, **incremental** fact/marts,
  and 40+ tests (uniqueness, relationships, accepted values/ranges, custom
  reconciliation).
- Wrote a realistic **synthetic data generator** (numpy) producing INR-priced,
  Indian-geography data with seasonality, repeat-customer power-law behaviour and
  **deliberately injected defects**, plus 17 analytical SQL queries and a
  **Streamlit** dashboard (GMV, AOV, retention, delivery SLA); fully containerised
  with **Docker Compose** (Postgres, MinIO, Airflow, dashboard).

## Compact project description (for a portfolio or the top of the README)

> A production-shaped (but honestly synthetic) **e-commerce data engineering
> platform**: a batch ELT pipeline that ingests eight messy source feeds into a
> Bronze/Silver/Gold lakehouse, cleans them with PySpark, models a tested star
> schema in dbt (with SCD2 and incremental loads), orchestrates everything with
> Airflow, and serves KPIs through a Streamlit dashboard — all runnable locally
> with Docker Compose.

## Skills this project demonstrates (for a skills line)

Data modelling (star schema, SCD2, grain design) · PySpark (windows, broadcast
joins, partitioning, incremental) · dbt (models, tests, snapshots, macros,
lineage) · Airflow (DAGs, retries, scheduling) · PostgreSQL · data quality /
validation · Docker Compose · Python (pandas, numpy) · SQL (window functions,
CTEs) · data-lake/S3 (MinIO) · Streamlit.

## A note on honesty (why this reads well in interviews)

This project deliberately **does not** claim billions of rows or real production
traffic. It claims exactly what it is: a well-engineered, end-to-end pipeline on
realistic synthetic data, with genuine trade-offs documented. That honesty is
defensible under questioning — every bullet maps to code you can open and explain.
