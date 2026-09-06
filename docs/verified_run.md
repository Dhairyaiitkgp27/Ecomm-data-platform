# Verified run log

The full pipeline was executed with the **real engines** (Apache Spark 3.5,
PostgreSQL 16, dbt 1.8) on the **full ~200k-order dataset**. This file records the
actual results. Reproduce locally with `make up` (Airflow) or the manual sequence
below; CI (`.github/workflows/ci.yml`) reruns it on every push.

## Sequence executed
```
python -m generator.generate_data --output data/raw --seed 42        # ~1.0M rows, ~6s
python -m src.quality.run_quality --raw-dir data/raw                  # gate: PASS
spark-submit spark/jobs/bronze_to_silver.py --input data/raw --output data/silver   # REAL Spark
python scripts/load_to_staging.py --silver data/silver               # -> Postgres staging
cd dbt && dbt deps && dbt snapshot && dbt run && dbt run && dbt test  # build + incremental + tests
psql ... -f sql/analytics/business_queries.sql                       # all 17 queries
```

## Results

**Volumes generated (full run):**
```
customers 80,240 | sellers 2,000 | products 15,000 | orders 201,000
order_items 354,937 | payments 200,000 | shipments 178,219 | returns 8,455
TOTAL ~1.04M rows across 8 tables | 72 MB raw CSV
generation ~6s (~290k rows/s) | quality gate <1s (~1.4M rows/s)
```

**Data-quality gate (PASS):** rejects duplicate PKs (customers 480, orders 2,000)
and non-positive quantities (1,775); quarantines orphan order_items (1,061),
impossible-date shipments (1,748) and negative-price products (37).

**dbt build:** 24 models + 1 SCD2 snapshot built successfully; a **second
`dbt run` is a clean no-op on the incremental models** (idempotent).

**dbt test:** `PASS=70 WARN=0 ERROR=0 SKIP=0` — all 70 data tests pass.

**Warehouse KPIs (marts.fact_orders):**
```
GMV ₹342.47 crore | valid_orders 186,129 | AOV ₹17,281
cancellation 6.94% | return rate 4.05%
marts: 11 tables (6 dims + 5 facts) | analytics: 5 tables
```

**Return rate by category (top 5):** Fashion 2.73%, Electronics 2.37%,
Sports & Fitness 2.36%, Home & Kitchen 2.34%, Appliances 2.32%.

**SCD Type 2 demonstration** — after changing one seller and re-snapshotting,
`marts.dim_seller_history` shows two versions:
```
seller_id  | rating | city   | business_type | valid_from | valid_to  | is_current
SELL000000 |   4.0  | Meerut | Individual    | <t0>       | <t1>      | f
SELL000000 |   4.4  | Pune   | Brand Store   | <t1>       | (current) | t
```

**Analytics SQL:** all 17 queries in `sql/analytics/business_queries.sql` execute
against the marts with no errors.

## Bugs found by executing (that static checks missed)
1. **`round(double precision, int)` does not exist** in Postgres — `avg()` returns
   double; fixed with `::numeric` casts in `delivery_performance` and several
   ad-hoc queries.
2. **Incremental model self-reference** — `daily_sales` filtered on a column it
   didn't output, so it failed only on the *second* run; fixed by carrying
   `order_date_key` through.
3. **Nullable-int→float load quirk** — `is_late` with NULLs loads as float, making
   `sum()` double; handled with a numeric cast in the affected query.

These are exactly the failures that "it compiles / refs resolve" cannot catch —
the reason the pipeline was run, not just validated.
