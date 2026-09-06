# Interview questions & answers

Prepared Q&A for defending this project. Answers are written to be spoken by a
fresher — technically correct but explainable. Everything here reflects what the
code actually does.

## Design & tooling

**1. Give me a 60-second overview of the project.**
It's a batch ELT pipeline for an Indian e-commerce business. Eight raw CSV feeds
land in a MinIO data lake (Bronze), a Python quality gate rejects/quarantines
bad rows, PySpark cleans and standardises them into Silver Parquet, that's loaded
into PostgreSQL staging, and dbt builds a star schema plus business marts (Gold)
with tests and documentation. Airflow orchestrates it, and a Streamlit dashboard
reads the marts. The data is synthetic but realistic, and I deliberately inject
data-quality problems so the pipeline has real issues to handle.

**2. Why PySpark instead of just pandas or SQL?**
Spark does the heavy, row-level, distributed work: deduplicating across the whole
feed with window functions, per-row standardisation, and writing partitioned
Parquet. That set of operations is painful in SQL and doesn't scale in pandas.
The code runs on one machine now but the same code runs on a cluster unchanged.

**3. Why PostgreSQL as the warehouse?**
It's free, ubiquitous, and perfect for this scale (~200k orders). The modelling
patterns — star schema, incremental models, SCD2 — are identical to what you'd do
on Snowflake/BigQuery/Redshift, so the skills transfer. For much larger data I'd
swap in a columnar cloud warehouse; the dbt models would barely change.

**4. Why dbt on top of Spark? Isn't that redundant?**
No — they do different jobs. Spark does imperative, distributed cleaning that's
awkward in SQL. dbt does declarative, set-based modelling that's awkward in Spark:
joining into a star schema, incremental merges, and — importantly — built-in
testing, documentation and lineage. Using both means each tool does what it's
best at, with no overlap.

**5. Why Airflow?**
It's the standard batch orchestrator: scheduling, task dependencies, retries with
backoff, and control over backfills. My DAG is a simple linear flow so failures
are easy to locate and re-run.

**6. Why a data lake (MinIO) and a warehouse — why not one?**
The lake (Bronze/Silver) cheaply stores raw and cleaned data in open formats
(CSV/Parquet) and decouples ingestion from modelling. The warehouse serves fast
SQL analytics. This is the standard lakehouse-style split. MinIO is just
S3-compatible storage I can run locally.

## Dimensional modelling

**7. Explain the star schema you built.**
Central fact tables (`fact_orders`, `fact_order_items`, `fact_payments`,
`fact_shipments`, `fact_returns`) surrounded by conformed dimensions
(`dim_customer`, `dim_product`, `dim_seller`, `dim_date`, `dim_location`,
`dim_seller_history`). Facts hold measures and foreign keys; dimensions hold
descriptive attributes. It's denormalised for fast, simple analytical joins.

**8. What is the grain of each fact table?**
`fact_orders` — one row per order. `fact_order_items` — one row per product line
within an order (the finest grain). `fact_payments` — one row per payment record.
`fact_shipments` — one row per shipment. `fact_returns` — one row per returned
line. Being explicit about grain is the first thing I decide for any fact table,
because it determines what a row means and how you aggregate.

**9. Why separate `fact_orders` and `fact_order_items`?**
Different grains answer different questions. Order-level metrics (AOV,
cancellation rate) live in `fact_orders`; product/seller-level analysis needs the
line grain in `fact_order_items`. Keeping them separate avoids double-counting
and fan-out when you join.

**10. What are conformed dimensions?**
Dimensions shared across facts with the same meaning — e.g. `dim_date` and
`dim_customer` are used by orders, items, payments and returns alike. That's what
lets you compare metrics consistently across facts.

**11. Surrogate keys vs natural keys — what did you use and why?**
Most dimensions use the natural business key (e.g. `customer_id`) because it's
stable and readable, which keeps joins simple in dbt. `dim_date` uses an integer
`YYYYMMDD` key, and `dim_location`/`dim_seller_history` use surrogate keys
(hashed) — the SCD2 history dim needs one because a seller has multiple versions,
so the natural key alone isn't unique.

**12. What is a degenerate dimension? Do you have one?**
A dimension attribute stored on the fact with no separate dimension table —
`order_id`, `order_status` and `payment_type` on `fact_orders`. They're useful
for filtering/grouping but don't warrant their own table.

## Slowly Changing Dimensions

**13. What is SCD Type 2 and where do you use it?**
SCD2 keeps history: when a tracked attribute changes, you close the old row and
insert a new version with effective-from/to dates. I use it for sellers
(`dim_seller_history`) so I can see a seller's rating/city/business-type *as of*
any past date, via a dbt snapshot.

**14. How does the dbt snapshot implement SCD2?**
`dbt/snapshots/sellers_snapshot.sql` uses the `check` strategy on chosen columns.
On each `dbt snapshot`, dbt compares the current source row to the latest snapshot
row; if a tracked column changed it sets `dbt_valid_to` on the old version and
inserts a new current one. No hand-written merge logic. `dim_seller_history`
exposes `effective_start_date`, `effective_end_date` and `is_current`.

**15. SCD1 vs SCD2 — when each?**
SCD1 overwrites (no history) — fine when you only care about current state, like
a customer's current segment. SCD2 keeps history — needed when point-in-time
correctness matters, like a seller's rating at the time of a past order. I keep a
current-state `dim_seller` (SCD1-style) for easy joins **and** `dim_seller_history`
(SCD2) for history.

## Incremental & idempotency

**16. What's your incremental strategy?**
Two levels. Spark keeps a high-water mark on `order_purchase_ts`, so in
incremental mode it only processes newer orders. In dbt, `fact_order_items` and
`daily_sales` are incremental models using `delete+insert` on their keys, so only
recent partitions are rebuilt rather than the whole table.

**17. How do you handle late-arriving data?**
The incremental dbt models look back a few days (`order_date >= max(order_date) -
3 days`) before merging, so rows that arrive slightly late still get picked up and
merged on the unique key instead of being missed at the boundary.

**18. What does idempotency mean here and how do you get it?**
Re-running the pipeline for the same input produces the same result — no
duplicates, no drift. Ingestion writes content-addressed keys
(`<source>__<md5>.csv`), so re-landing identical data is a no-op. dbt incremental
models merge on unique keys (delete+insert), so re-running a window overwrites
rather than appends.

**19. How do you handle duplicate events/records?**
The quality gate flags duplicate primary keys, and Spark removes them
deterministically with a `row_number()` window keyed by the PK, ordered so the
latest record wins. So even if a source double-sends a row, exactly one survives.

## Spark internals

**20. What is a shuffle and why care?**
A shuffle moves data across partitions/nodes for wide operations (group-by, join,
window) — it's the expensive part of Spark because it involves disk and network.
I minimise shuffles: explicit schemas, semi/anti-joins for existence checks, a
broadcast join for the small dimension, and a sensible `shuffle.partitions`.

**21. What's a broadcast join and when do you use it?**
When one side of a join is small, Spark can send ("broadcast") it to every node so
the large side isn't shuffled — a map-side join. I broadcast the product
dimension when enriching the large `order_items` fact. I wrap it in
`F.broadcast()` to make the intent explicit.

**22. When would broadcasting be a bad idea?**
If the "small" side isn't actually small, broadcasting can blow up executor
memory. It's only safe when the broadcast side comfortably fits in memory.

**23. Why partition Silver by year/month?**
Date-filtered reads (most analytics) can skip irrelevant partitions — partition
pruning — so they read far less data. It also makes incremental appends land in
the correct partitions. I don't partition tiny dimensions, to avoid the
small-files problem.

**24. When do you cache in Spark, and did you?**
Cache a DataFrame that's reused multiple times and is expensive to recompute. I
cache the cleaned `orders` DataFrame because it feeds several semi-joins and the
watermark calc; recomputing its dedup+window each time would be wasteful. I
`unpersist()` it after, and I don't cache single-use DataFrames.

**25. `repartition` vs `coalesce`?**
`repartition(n)` reshuffles into n partitions (can increase or balance them);
`coalesce(n)` reduces partitions without a full shuffle (cheaper, for shrinking).
Use coalesce to cut down output files, repartition to rebalance skew or increase
parallelism.

## Orchestration & reliability

**26. How does the DAG recover from failure?**
Tasks have `retries=2` with exponential backoff, so transient failures self-heal.
Each stage is independently re-runnable and idempotent, so a failed run can be
cleared and retried from the failed task without corrupting data. `catchup=False`
and `max_active_runs=1` prevent a backlog storm.

**27. What happens if the quality gate fails?**
With `DQ_FAIL_FAST=true`, the `quality_gate` task raises, the DAG run fails, and
nothing downstream executes — so bad data never reaches Silver or the marts. The
report and quarantined rows are available for investigation.

**28. How would you monitor data quality in production?**
Emit the gate's metrics (rejected/quarantined counts and rates per table) to a
monitoring system and alert on threshold breaches or sudden changes. dbt test
results can be published similarly. You'd track quarantine volume over time as a
data-health signal.

**29. How is configuration managed? Any secrets in code?**
Everything is environment-driven (`src/utils/config.py`, `.env`, compose env).
There are no hardcoded paths or credentials; local defaults exist for convenience
but are overridden by env in Docker.

## Scaling & trade-offs

**30. How would this scale to 100× the data?**
Run Spark on a real cluster (same code), raise shuffle partitions and size
partitions to ~128–200 MB, move the warehouse to a columnar cloud DW (Snowflake/
BigQuery), and keep dbt models mostly as-is. Storage is already in scalable object
storage. The watermark file would become a metadata table for multi-writer safety.

**31. What are the honest limitations of this project?**
The data is synthetic (though realistic), and I can't run the full Spark+Airflow+
Postgres+MinIO stack on a laptop for free indefinitely — but every layer is real
code, the generator and quality gate run and are tested, and the dbt project is
internally consistent. It's a portfolio/learning project, not a production system
handling real traffic.

**32. If you had one more week, what would you add?**
Great Expectations or Soda for richer quality reporting, CI (GitHub Actions) to
run the tests and `dbt build` on every push, dbt exposures + a lineage graph in
the README, and a small streaming variant (Kafka → Spark Structured Streaming)
for near-real-time order events.

**33. Why synthetic data — isn't real data better?**
There's no public *Indian* dataset with all eight related tables and the messy
quality issues I wanted to handle. Generating it gave me control over realistic
distributions **and** let me inject specific, known defects to prove the quality
and Spark layers work. The generator is itself an engineering artifact.
