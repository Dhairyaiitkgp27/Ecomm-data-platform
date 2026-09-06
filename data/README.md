# Data

## What lives here

- `sample/` — a **small committed sample** (~3,000 orders and related rows) so the
  repo is explorable and the dashboard's *Sample CSV* mode works immediately.
- `raw/`, `silver/`, `quarantine/` — **generated locally, git-ignored.** Produced
  by `make generate` / the pipeline; not committed to keep the repo small.

## Data provenance — honest summary

**All data in this project is synthetically generated.** There is no private or
proprietary data, and none of it comes from Flipkart, Amazon, or any real company.

| Aspect | Source |
|---|---|
| **Schema design** | Modelled on the public **Olist Brazilian E-Commerce** dataset structure (customers / orders / order_items / products / sellers / payments), which is the de-facto reference schema for e-commerce analytics projects. |
| **All row data** | **100% synthetic**, produced by `generator/` using `numpy`. |
| **Indian characteristics** | Hand-curated reference data in `generator/reference_data.py`: real Indian cities mapped to states (population-weighted), an Indian category taxonomy with INR price bands, COD/UPI/card payment mix, and Indian names. |

### Why synthetic rather than a downloaded dataset?

The project targets **Indian** e-commerce (INR pricing, Indian geography, COD vs
prepaid behaviour, festival-season demand). No suitable public *Indian* dataset
exists with all eight related tables and the messy real-world data-quality
issues this pipeline is built to handle. Generating the data:

1. gives full control over **realistic distributions and relationships**
   (seasonality, repeat-customer power law, category-driven return rates,
   distance-driven delivery times), and
2. lets us **inject specific, known data-quality defects** so the quality and
   Spark layers have something real to detect, correct and quarantine.

The generator itself is a first-class engineering artifact — see
`generator/generate_data.py` and `docs/` for the methodology.

### How the synthetic data is generated (high level)

- Dimensions first (customers, sellers, products) with weighted geography and
  category-specific lognormal price bands.
- Orders sample customers via a **Zipf distribution** (so some customers are
  repeat buyers) and dates via a **seasonality-weighted** calendar (weekend +
  festival uplift).
- Order items, payments, shipments and returns are derived with **referential
  integrity** and realistic dependencies (e.g. delivery time depends on the
  destination city tier; return probability depends on product category).
- Finally, **deliberate defects** are injected at controlled rates: duplicate
  keys, nulls, dirty city/category/payment values, impossible dates, bad
  quantities, negative prices and orphan foreign keys.

### Reproducibility

Everything is seeded. `make generate-sample` reproduces the committed sample
byte-for-byte; `make generate` produces the full ~200k-order dataset.
