# Spark: design & optimizations

The Bronze→Silver job (`spark/jobs/bronze_to_silver.py`) is where the heavy,
distributed work happens. Techniques used and **why**:

## 1. Explicit schemas (no `inferSchema`)
Raw data is dirty and large; inferring schema means an extra full pass and risky
type guesses. We declare `StructType`s in `spark/jobs/common.py` and read dirty
columns as strings, casting deliberately later. Faster and predictable.

## 2. Deterministic de-duplication with a window
Duplicates are removed with `row_number()` over `Window.partitionBy(key)
.orderBy(order_col desc)`, keeping the latest record per key. This is
deterministic (unlike `dropDuplicates`, which is arbitrary on ties) and mirrors
how you would deduplicate CDC/event data in production.

## 3. Broadcast join for the small dimension
`order_items` (large) is enriched with product attributes (small) using
`F.broadcast(products)`. Broadcasting the small side avoids a shuffle of the
large fact — the classic map-side join optimization. Guarding with `broadcast()`
makes the intent explicit rather than relying on the auto-broadcast threshold.

## 4. Minimise and localise shuffles
Wide operations (dedup windows, group-bys) cause shuffles. We keep them to the
minimum needed, set `spark.sql.shuffle.partitions` to a value appropriate for
local/dev volumes (default 8; raise on a cluster), and prefer semi/anti-joins
(`left_semi`, `left_anti`) over full joins when we only need existence checks
(orphan detection, valid-order filtering).

## 5. Caching a reused DataFrame — only where justified
The cleaned `orders` DataFrame is `cache()`d because it is reused several times
(valid-order-id semi-joins for items/payments/shipments/returns **and** the
watermark computation). Without caching, the non-trivial dedup + window would be
recomputed for each downstream use. It is `unpersist()`d at the end. We do *not*
cache DataFrames used once — caching indiscriminately wastes memory.

## 6. Partitioned Parquet output
Large, time-series facts (`orders`, and by extension items) are written
partitioned by `order_year`/`order_month`. This gives **partition pruning** for
date-filtered reads and makes incremental appends land in the right partitions.
Small dimensions are written unpartitioned (partitioning tiny data just creates
many small files — the small-file problem).

## 7. Window function for a real business need
A per-customer chronological `row_number()` produces `customer_order_seq` and
`is_repeat_order`, consumed downstream for new-vs-repeat revenue analysis — a
window function used because the problem is inherently ordered-within-group, not
for its own sake.

## 8. Incremental processing (high-water mark)
In `--incremental` mode the job reads the last processed `order_purchase_ts` from
a watermark file, filters to newer orders only, appends to Silver, and advances
the watermark. This avoids reprocessing history every run. A short look-back
window is used downstream (in dbt) to absorb late-arriving rows.

## 9. Derived columns computed once
Business metrics (`delivery_days`, `is_late`, `is_cod`, `line_total_inr`) are
computed in Silver so every downstream consumer (dbt, dashboard, ad-hoc SQL)
reuses them instead of recomputing — consistent definitions, less repeated work.

## Scaling notes (how this changes with real volume)
- Raise `spark.sql.shuffle.partitions` (rule of thumb ~2–3× total cores, or size
  partitions to ~128–200 MB).
- Run on a cluster (YARN/K8s/EMR) with `--master` pointed at it; the code is
  unchanged.
- Consider Z-ordering / bucketing on `order_id` if item-level joins dominate.
- Replace the watermark file with a metadata table for multi-writer safety.
