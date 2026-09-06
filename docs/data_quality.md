# Data quality strategy

Data quality is enforced at **three** points, each catching a different class of
problem, so bad data cannot silently reach the marts.

1. **Ingestion (schema presence)** — a file missing required columns is rejected
   wholesale before landing.
2. **Quality gate (Python, pre-Spark)** — row-level structural checks that
   *reject / quarantine / correct / allow*, and **fail the pipeline** if a table
   breaches its thresholds. Code: `src/quality/`.
3. **dbt tests (post-Gold)** — declarative tests on the modelled star schema
   (uniqueness, not-null, relationships, accepted values, accepted ranges, and
   custom reconciliation tests). Code: `dbt/models/**/*.yml`, `dbt/tests/`.

## The four dispositions

| Disposition | Meaning | Example defects | Where handled |
|---|---|---|---|
| **REJECT** | Row is unusable; drop and count | null primary key; duplicate PK; non-positive quantity | gate flags; Spark drops |
| **QUARANTINE** | Set aside for review; excluded from Silver | orphan FK (order_item → missing order); delivered-before-shipped; absurd future date | gate writes `*_quarantine.csv`; Spark writes `_quarantine/` |
| **CORRECT** | Keep the row; fix the value downstream | dirty city (`BANGALORE`,`Bengaluru `→`Bengaluru`); malformed category; messy payment status (`PAID`,`success`→`paid`) | Spark canonicalises in Bronze→Silver |
| **ALLOW** | Keep as-is; record a warning only | null non-critical field; unusually long delivery | flagged, not blocked |

## Injected defects → disposition (traceability)

The generator injects known defects at controlled rates (`configs/data_generation.yml`).
Each maps to a disposition the pipeline is built to apply:

| Injected defect | Rate | Disposition |
|---|---|---|
| Duplicate `order_id` rows | 0.5% | REJECT (dedup, keep latest) |
| Duplicate `customer_id` rows | 0.3% | REJECT (dedup) |
| Null `city` / `state` | 2% | ALLOW → filled `Unknown` |
| Dirty city names (aliases/casing) | 6% | CORRECT (canonical map) |
| Malformed `category` (casing/`&`↔`and`) | 4% | CORRECT (normalise) |
| Messy `payment_status` variants | 10% | CORRECT (canonical map) |
| Non-positive `quantity` | 0.5% | REJECT |
| Negative `mrp_inr` | 0.2% | QUARANTINE/null the measure |
| Orphan `order_items` (bad `order_id`) | 0.3% | QUARANTINE |
| Impossible / future dates | 1% | QUARANTINE |
| Null `payment_value` | 1% | ALLOW → 0 + `payment_value_missing` flag |
| Null `delivered_ts` on delivered | 2% | ALLOW |

## Fail-fast thresholds

Set in `src/quality/rules.py`:

- **REJECT** rate > **5%** in any table ⇒ pipeline stops.
- **QUARANTINE** rate > **10%** in any table ⇒ pipeline stops.
- CORRECT / ALLOW never fail the pipeline.

When `DQ_FAIL_FAST=true` (default) the Airflow `quality_gate` task raises, so the
run fails loudly and no partial/bad data flows to Silver.

## dbt tests (examples)

- **Uniqueness / not-null** on every dimension PK and fact grain.
- **Relationships** (referential integrity) from facts to dimensions.
- **Accepted values** for `order_status`, `payment_status`, `customer_segment`.
- **Accepted range** for `quantity` (≥1), `gmv_inr` (≥0), `return_rate` (0–1).
- **Custom singular tests**: `assert_order_value_reconciles` (order total = Σ item
  line totals within tolerance) and `assert_no_future_order_dates`.
