# E-Commerce Data Engineering Platform (India) 🛒

[![CI](https://github.com/<your-username>/ecommerce-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/<your-username>/ecommerce-data-platform/actions/workflows/ci.yml)

An **end-to-end batch ELT pipeline** for a (synthetic) Indian e-commerce business.
Eight messy source feeds are ingested into a **Bronze/Silver/Gold lakehouse**,
cleaned with **PySpark**, modelled into a tested **star schema** with **dbt**
(including **SCD Type 2** and incremental loads), orchestrated with **Airflow**,
and served through a **Streamlit** dashboard — all runnable locally with
**Docker Compose**.



---

## Table of contents
1. [Highlights](#highlights) · 2. [Architecture](#architecture) · 3. [Business problem](#business-problem) ·
4. [Data sources](#data-sources) · 5. [Data model](#data-model) · 6. [Pipeline flow](#pipeline-flow) ·
7. [Tech stack & why](#tech-stack--why-each) · 8. [Data quality](#data-quality) · 9. [Incremental](#incremental-strategy) ·
10. [Spark optimizations](#spark-optimizations) · 11. [Airflow DAG](#airflow-dag) · 12. [dbt project](#dbt-project) ·
13. [Example SQL](#example-sql) · 14. [Dashboard](#dashboard) · 15. [Testing](#testing) ·
16. [Project structure](#project-structure) · 17. [Setup & how to run](#setup--how-to-run) ·
18. [What runs vs simulated](#what-actually-runs-vs-what-is-simulated) · 19. [Limitations & future work](#limitations--future-improvements)

---

## Highlights
- **Medallion architecture**: Bronze (raw) → Silver (cleaned) → Gold (star schema).
- **Real data-quality handling**: a reject / quarantine / correct / allow framework
  that **fails the pipeline** on threshold breaches, plus 40+ dbt tests.
- **PySpark** cleaning: window de-duplication, standardisation, **broadcast joins**,
  **partitioned Parquet**, **incremental** high-water-mark processing.
- **dbt** star schema: 5 facts + 6 dimensions, **SCD2 seller history** via snapshot,
  incremental fact & marts, macros, tests, docs/lineage.
- **Airflow** DAG with retries, fail-fast quality gate, and clean stage separation.
- **Synthetic data generator** with realistic Indian distributions and *deliberately
  injected* defects — an engineering artifact in its own right.
- **17 analytical SQL queries** + a **Streamlit** dashboard (works on the marts, or on
  the committed sample before you run anything).
- Fully **containerised** (Postgres, MinIO, Airflow, dashboard) via Docker Compose.

## Architecture
Medallion ELT; each tool does the one job it's best at (no Spark/dbt overlap).

```
Sources (8 CSVs) ─▶ Ingestion (Python, idempotent)
     ─▶ BRONZE (MinIO, raw)  ─▶ Quality Gate (Python: reject/quarantine/correct/allow, fail-fast)
     ─▶ PySpark Bronze→Silver (dedup, standardise, partitioned Parquet)
     ─▶ SILVER (MinIO)  ─▶ PySpark load ─▶ PostgreSQL `staging`
     ─▶ dbt (staging→intermediate→marts) + dbt snapshot (SCD2)
     ─▶ GOLD: `marts` (dims+facts) & `analytics` (business marts)  ─▶ Streamlit
Airflow orchestrates the whole DAG.
```
Full detail + diagram: **[docs/architecture.md](docs/architecture.md)**.

## Business problem
An online marketplace needs a clean analytical warehouse to answer questions its
raw operational feeds can't easily support:
- What are **GMV, orders and AOV**, and how do they trend month-over-month?
- Which **products, sellers, categories and states** drive revenue?
- What are the **cancellation and return rates**, and where are returns rising?
- How do **COD vs prepaid** orders differ? How is **delivery SLA** (on-time rate)?
- Who are our **repeat customers** and what share of revenue do they drive
  (retention/cohorts)?

All of these are answered by the marts and the queries in
[`sql/analytics/business_queries.sql`](sql/analytics/business_queries.sql).

## Data sources
Eight related feeds (synthetic): **customers, sellers, products, orders,
order_items, payments, shipments, returns**. Schema is inspired by the public
**Olist** e-commerce dataset; **all rows are generated** with realistic Indian
characteristics (INR pricing, population-weighted cities/states, COD/UPI mix,
festival seasonality, category-driven returns). A small sample is committed under
`data/sample/`. Full provenance & methodology:
**[data/README.md](data/README.md)**.

## Data model
A **star schema** at the Gold layer.

- **Dimensions**: `dim_date`, `dim_customer`, `dim_product`, `dim_seller`
  (current), `dim_seller_history` (**SCD2**), `dim_location`.
- **Facts** (grain in brackets): `fact_orders` (order), `fact_order_items`
  (order line), `fact_payments` (payment), `fact_shipments` (shipment),
  `fact_returns` (returned line).
- **Business marts**: `daily_sales`, `seller_performance`, `product_performance`,
  `customer_summary`, `delivery_performance`.

Column-level detail, keys and grains: **[docs/data_dictionary.md](docs/data_dictionary.md)**.

## Pipeline flow
1. **Ingest** → land raw CSVs in Bronze (MinIO) with content-addressed,
   `ingest_date`-partitioned keys + a stable `latest/` pointer. Idempotent.
2. **Quality gate** → structural checks; quarantine bad rows; **fail fast** if a
   table breaches thresholds (nothing bad reaches Silver).
3. **Bronze→Silver (PySpark)** → dedup, standardise cities/categories/statuses,
   null-handling, derived metrics, **partitioned Parquet**.
4. **Silver→Staging (PySpark JDBC)** → load cleaned tables into Postgres `staging`.
5. **dbt snapshot** → capture SCD2 seller versions.
6. **dbt run** → build staging views → intermediate → **marts** (dims, facts,
   analytics).
7. **dbt test** → run all data tests.
8. **Warehouse checks** → final assertions on the marts.

## Tech stack & why each
| Concern | Choice | Why |
|---|---|---|
| Language / generation | Python, numpy, pandas | Fast vectorised synthetic data; glue code |
| Object store (lake) | **MinIO** (S3-compatible) | Local, open formats, decouples ingest from modelling |
| Distributed transform | **PySpark** | Row-level, distributed cleaning that scales; same code on a cluster |
| Warehouse | **PostgreSQL** | Free, ubiquitous; patterns transfer to Snowflake/BigQuery |
| Modelling / tests | **dbt** | Set-based SQL modelling + built-in tests, docs, lineage |
| Orchestration | **Airflow** | Scheduling, dependencies, retries, backfill control |
| Serving | **Streamlit** | Runnable, reviewable dashboard inside Docker |
| Packaging | **Docker Compose** | One-command local stack |

Chose **Streamlit over Power BI** so the dashboard runs and is reviewable in the
same containerised stack (no desktop dependency).

## Data quality
Enforced at **three** points: ingestion (schema presence), a **Python gate**
(reject/quarantine/correct/allow, fail-fast), and **dbt tests** on the marts. The
generator injects known defects (duplicate keys, nulls, dirty city/category/
payment values, impossible dates, bad quantities, orphan FKs) so the framework
has real problems to handle. Policy, thresholds and the defect→disposition map:
**[docs/data_quality.md](docs/data_quality.md)**.

## Incremental strategy
- **Spark**: high-water mark on `order_purchase_ts`; `--incremental` processes only
  newer orders and advances the watermark.
- **dbt**: `fact_order_items` and `daily_sales` are **incremental** (`delete+insert`
  on keys) with a short **look-back window** to absorb late-arriving rows.
- **Idempotency**: content-addressed ingestion + merge-on-key models mean re-runs
  don't duplicate data.

## Spark optimizations
Explicit schemas, deterministic **window de-duplication**, **broadcast join** for
the small product dimension, shuffle minimisation (semi/anti-joins), justified
**caching** of the reused `orders` DataFrame, and **partitioned Parquet** output.
Details & scaling notes: **[docs/spark_optimizations.md](docs/spark_optimizations.md)**.

## Airflow DAG
`dags/ecommerce_pipeline_dag.py` — `ecommerce_batch_pipeline`:
`ingest → quality_gate → spark_bronze_to_silver → spark_silver_to_warehouse →
dbt_snapshot → dbt_run → dbt_test → warehouse_quality_check`. Daily schedule,
`retries=2` with backoff, `catchup=False`, `max_active_runs=1`. Mixes
`PythonOperator` (Python stages) and `BashOperator` (spark-submit, dbt CLI).
Toggle incremental via the Airflow Variable `pipeline_mode=incremental`.

## dbt project
- **Layers**: `staging` (views) → `intermediate` (ephemeral) → `marts.core`
  (dims+facts, tables) → `marts.analytics` (business marts, tables).
- **SCD2**: `snapshots/sellers_snapshot.sql` (check strategy) →
  `dim_seller_history` with `effective_start/end_date`, `is_current`.
- **Incremental**: `fact_order_items`, `daily_sales`.
- **Tests**: uniqueness, not-null, relationships, accepted values/ranges, and
  custom singular tests (order-total reconciliation, no future dates).
- **Macros**: `date_key`, `safe_divide`, schema naming.
- **Lineage/docs**: `make dbt-docs` (generate + serve).

Demonstrate SCD2 end-to-end: `dbt snapshot` → apply
[`sql/scd2_demo_update.sql`](sql/scd2_demo_update.sql) → `dbt snapshot` again →
that seller now has two versions in `dim_seller_history`.

## Example SQL
17 analytical queries (CTEs + window functions) in
[`sql/analytics/business_queries.sql`](sql/analytics/business_queries.sql):
monthly GMV + MoM growth, 7-day moving average, AOV rollups, top products/sellers,
repeat-customer revenue share, cohort retention, cancellation & return rates,
COD vs prepaid, state-wise sales, delivery SLA, rising-returns detection,
in-state seller ranking, new-vs-repeat revenue split, and more.

## Dashboard
`dashboard/app.py` (Streamlit) with a sidebar toggle:
- **Warehouse (Postgres)** — reads the dbt marts (the real deliverable).
- **Sample CSV** — computes headline KPIs from `data/sample` so it's viewable
  **before** running the pipeline.

Shows GMV / orders / AOV / cancellation / return rate, monthly trends, top
categories & sellers, state-wise sales and delivery performance.

## Testing
`pytest` suite (unit + integration) covering the generator, the pure
data-quality validators, ingestion schema checks, and env config, plus an
integration **smoke test** (generate → CSV → quality gate). dbt ships its own data
tests. Run: `make test`.

## Project structure
```
ecommerce-data-platform/
├── generator/            # synthetic data generator (numpy) + Indian reference data
├── src/
│   ├── ingestion/        # idempotent Bronze landing + source schemas
│   ├── quality/          # reject/quarantine/correct/allow gate (pure validators)
│   └── utils/            # env-driven config, logging, S3/MinIO IO
├── spark/jobs/           # Bronze→Silver + Silver→Warehouse PySpark jobs
├── dbt/                  # staging→intermediate→marts, snapshots (SCD2), tests, macros
├── dags/                 # Airflow DAG
├── sql/analytics/        # 17 business queries
├── dashboard/            # Streamlit app
├── scripts/              # Silver→staging loader (Spark-free path, used by CI)
├── tests/                # pytest unit + integration
├── .github/workflows/    # CI: tests + full Spark→dbt→test pipeline on Postgres
├── docker/               # Dockerfiles, Postgres init, MinIO bucket bootstrap
├── configs/              # data-generation + pipeline config (YAML)
├── docs/                 # architecture, data quality, spark, interview Q&A, resume, dictionary
├── data/sample/          # small committed sample (8 CSVs)
├── docker-compose.yml    # Postgres + MinIO + Airflow + dashboard
├── Makefile              # one-line commands (make help)
└── requirements.txt
```

## Setup & how to run

### Prerequisites
- **Quickstart (no Docker):** Python 3.11+.
- **Full stack:** Docker + Docker Compose.

### Option A — Quickstart (see it work in ~1 minute, no Docker)
```bash
pip install -r requirements.txt          # or: pip install pandas numpy pytest
make generate-sample                      # writes ~3k-order sample to data/sample
make test                                 # runs the pytest suite (should pass)
make quality                              # run the DQ gate (point it at data/sample):
python -m src.quality.run_quality --raw-dir data/sample --quarantine-dir data/quarantine --no-fail-fast
streamlit run dashboard/app.py            # then pick "Sample CSV" in the sidebar
```

**No-Spark warehouse path:** if you have Postgres but not Spark, you can skip the
Spark job and load the sample straight into staging, then build the marts:
```bash
python scripts/load_to_staging.py --silver data/sample   # loads CSVs into staging
cd dbt && dbt deps && dbt run && dbt test                 # build + test the star schema
```


### Option B — Full pipeline (Docker Compose)
```bash
cp .env.example .env
make up            # builds + starts Postgres, MinIO, Airflow, dashboard
# Airflow UI  → http://localhost:8080  (admin / admin)
# MinIO UI    → http://localhost:9001  (minioadmin / minioadmin)
# Dashboard   → http://localhost:8501

# generate a full dataset into the mounted project, then run the DAG:
docker compose exec airflow-scheduler python -m generator.generate_data --output data/raw
make trigger       # triggers ecommerce_batch_pipeline
make logs          # watch it run
```

### Option C — Run stages manually (local, needs pyspark/dbt installed)
```bash
make generate                  # full dataset → data/raw
make spark-local               # Bronze→Silver (local paths)
make warehouse-load            # Silver→Postgres staging
make dbt-deps dbt-snapshot dbt-run dbt-test
```
Run `make help` to see every target.

## What actually runs vs what is simulated
This pipeline has been **executed end to end with the real engines** (Apache
Spark 3.5, PostgreSQL 16, dbt 1.8) on the **full ~200k-order dataset** — not just
statically checked.

| Component | Status |
|---|---|
| Synthetic data generator | ✅ **Ran** — ~1.0M rows across 8 tables in ~6 s |
| Data-quality gate | ✅ **Ran** — rejects dup PKs/bad qty, quarantines orphans/impossible dates |
| PySpark Bronze→Silver | ✅ **Ran** (real Spark) — dedup, standardise, partitioned Parquet written |
| Load Silver→Postgres staging | ✅ **Ran** — all 8 tables loaded |
| dbt snapshot / run | ✅ **Ran** — 24 models + SCD2 snapshot built; **incremental re-run verified** |
| dbt test | ✅ **70 / 70 tests pass** |
| 17 analytics SQL queries | ✅ **All 17 execute** against the marts |
| SCD2 history | ✅ **Demonstrated** — a seller change produced two versioned rows |
| pytest suite | ✅ **18 / 18 pass** |
| Full stack via **Airflow + MinIO** | ✅ Provided via Docker Compose (`make up`) — Airflow orchestration needs a Docker host, so run it on your machine |

### Verified run — real numbers from the built warehouse
*(full log: [docs/verified_run.md](docs/verified_run.md))*
On the full dataset (200,000 orders, 352,104 order-items, 80,000 customers,
15,000 products, 2,000 sellers):

- **GMV ₹342.5 crore**, **186,129** valid orders, **AOV ₹17,281**,
  **6.94%** cancellation, **4.05%** return rate.
- Warehouse built: **11** tables in `marts` (6 dims + 5 facts) + **5** in `analytics`.
- **70/70 dbt data tests pass**; the two incremental models produce identical
  results on a second run (idempotent).

Running the real engines also **surfaced and fixed three bugs** that static
checks miss — a Postgres `round(double, int)` type error, an incremental model
that referenced a column absent from its own output (fails only on the *second*
run), and a nullable-int-becomes-float loading quirk. That's exactly why
executing beats validating.

> The one thing not run here is **Airflow orchestration + MinIO**, because this
> build sandbox has no Docker host. The DAG's individual stages were all executed
> manually in sequence with the real engines; `make up` runs them under Airflow
> on your machine. Continuous integration (below) re-runs the whole
> Spark→load→dbt→test flow on every push.

### Continuous integration
[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs two jobs on every push:
a fast **tests** job (generator + quality gate + pytest across Python 3.10/3.11),
and a **warehouse** job that stands up a Postgres service and runs the *real*
pipeline — Spark Bronze→Silver → load → `dbt deps/snapshot/run/run/test` → execute
all analytics SQL. Green CI means the whole thing genuinely works, reproducibly.

## Limitations & future improvements
**Limitations (honest):**
- Data is synthetic (realistic, but not real traffic).
- Volumes are laptop-scale (~200k orders configurable); this is a learning
  project, not a production system.
- The dashboard is intentionally secondary to the data engineering.

**Future work:**
- CI (GitHub Actions) running `pytest` + `dbt build` on every push.
- Richer quality reporting (Great Expectations / Soda) with historical trends.
- A streaming variant (Kafka → Spark Structured Streaming) for near-real-time orders.
- dbt exposures + a published lineage graph; alerting on quality-metric drift.

## License
MIT — see [LICENSE](LICENSE). *(Update the copyright holder to your name.)*

---
*Interview prep and honest resume bullets for this project live in
[docs/interview_questions.md](docs/interview_questions.md) and
[docs/resume_bullets.md](docs/resume_bullets.md).*
