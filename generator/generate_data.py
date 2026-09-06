"""
Synthetic Indian e-commerce data generator.

Produces eight raw CSV files (customers, sellers, products, orders,
order_items, payments, shipments, returns) with:
  * realistic Indian geography, categories and INR price bands,
  * correlated behaviour (seasonality, repeat customers, delivery times that
    depend on origin/destination tier, category-driven return rates),
  * referential integrity across tables,
  * a controlled set of deliberately-injected data-quality defects.

The defects are what the downstream quality + Spark layers are built to detect,
correct or quarantine. See docs/ for the full data-quality policy.

Usage:
    python -m generator.generate_data --orders 200000 --output data/raw --seed 42
    python -m generator.generate_data --scale 0.015 --output data/sample   # tiny
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

from generator.config import GeneratorConfig, load_yaml_overrides
from generator import reference_data as ref

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | generator | %(message)s",
)
log = logging.getLogger("generator")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _lognormal_prices(rng, band, size):
    """Clipped lognormal centred on the median of a (min, median, max) band."""
    lo, med, hi = band
    sigma = 0.55
    raw = rng.lognormal(mean=np.log(med), sigma=sigma, size=size)
    return np.clip(raw, lo, hi).round(0)


def _pincode(rng, size):
    return rng.integers(110001, 855117, size=size).astype(str)


def _make_dates(rng, cfg: GeneratorConfig, n: int):
    """Sample n order timestamps across the window with weekend + festival uplift."""
    start = np.datetime64(cfg.start_date)
    end = np.datetime64(cfg.end_date)
    days = np.arange(start, end + np.timedelta64(1, "D"), dtype="datetime64[D]")
    dow = (days.astype("datetime64[D]").view("int64") - 4) % 7  # 0=Mon
    weight = np.ones(len(days))
    weight[dow >= 5] *= cfg.weekend_uplift  # weekend
    # Festival windows (approx BBD / Diwali / end-of-year sales)
    ds = pd.DatetimeIndex(days)
    month = np.asarray(ds.month)
    day = np.asarray(ds.day)
    festive = (
        ((month == 10) & (day >= 1) & (day <= 12))   # Big-sale kickoff
        | ((month == 10) & (day >= 20))              # Diwali build-up
        | ((month == 11) & (day <= 5))               # Diwali tail
        | ((month == 12) & (day >= 24))              # year-end
        | ((month == 1) & (day <= 3))                # new-year
    )
    weight[festive] *= cfg.festival_uplift
    weight = weight / weight.sum()
    chosen = rng.choice(len(days), size=n, p=weight)
    # add a random time-of-day
    secs = rng.integers(0, 86400, size=n)
    return pd.to_datetime(days[chosen]) + pd.to_timedelta(secs, unit="s")


# --------------------------------------------------------------------------- #
# Dimension-like source entities
# --------------------------------------------------------------------------- #
def gen_customers(rng, cfg: GeneratorConfig) -> pd.DataFrame:
    n = cfg.n_customers
    idx = np.arange(len(ref.CITIES))
    weights = np.array([c[3] for c in ref.CITIES], dtype=float)
    weights /= weights.sum()
    pick = rng.choice(idx, size=n, p=weights)
    cities = np.array([ref.CITIES[i][0] for i in pick])
    states = np.array([ref.CITIES[i][1] for i in pick])
    fn = rng.choice(ref.FIRST_NAMES, size=n)
    ln = rng.choice(ref.LAST_NAMES, size=n)
    names = np.char.add(np.char.add(fn.astype(str), " "), ln.astype(str))
    emails = np.array([
        f"{f.lower()}.{l.lower()}{rng.integers(1, 9999)}@example.in"
        for f, l in zip(fn, ln)
    ])
    signup = _make_dates(rng, cfg, n) - pd.to_timedelta(rng.integers(0, 400, n), unit="D")
    return pd.DataFrame({
        "customer_id": [f"CUST{ i:08d}".replace(" ", "") for i in range(n)],
        "customer_name": names,
        "email": emails,
        "phone": [f"9{rng.integers(100000000, 999999999)}" for _ in range(n)],
        "city": cities,
        "state": states,
        "pincode": _pincode(rng, n),
        "signup_date": signup.date,
        "customer_segment": rng.choice(ref.CUSTOMER_SEGMENTS, size=n, p=[0.35, 0.35, 0.22, 0.08]),
    })


def gen_sellers(rng, cfg: GeneratorConfig) -> pd.DataFrame:
    n = cfg.n_sellers
    idx = np.arange(len(ref.CITIES))
    weights = np.array([c[3] for c in ref.CITIES], dtype=float)
    weights /= weights.sum()
    pick = rng.choice(idx, size=n, p=weights)
    cities = np.array([ref.CITIES[i][0] for i in pick])
    states = np.array([ref.CITIES[i][1] for i in pick])
    tiers = np.array([ref.CITIES[i][2] for i in pick])
    return pd.DataFrame({
        "seller_id": [f"SELL{i:06d}" for i in range(n)],
        "seller_name": [f"{rng.choice(ref.BRANDS)} {rng.choice(['Enterprises','Traders','Retail','Store','Mart'])}"
                        for _ in range(n)],
        "seller_city": cities,
        "seller_state": states,
        "seller_tier": tiers,
        "seller_pincode": _pincode(rng, n),
        "onboarded_date": (_make_dates(rng, cfg, n)
                           - pd.to_timedelta(rng.integers(30, 900, n), unit="D")).date,
        "seller_rating": np.round(rng.normal(4.0, 0.5, n).clip(1.0, 5.0), 1),
        "business_type": rng.choice(ref.BUSINESS_TYPES, size=n, p=[0.4, 0.35, 0.15, 0.10]),
    })


def gen_products(rng, cfg: GeneratorConfig) -> pd.DataFrame:
    n = cfg.n_products
    cats = list(ref.CATEGORIES.keys())
    shares = np.array([ref.CATEGORIES[c]["weight_share"] for c in cats], dtype=float)
    shares /= shares.sum()
    cat_pick = rng.choice(len(cats), size=n, p=shares)
    category = np.array([cats[i] for i in cat_pick])
    subcat, price, weight_g = [], np.empty(n), np.empty(n)
    for i, c in enumerate(cats):
        mask = cat_pick == i
        k = int(mask.sum())
        if k == 0:
            continue
        meta = ref.CATEGORIES[c]
        subcat_arr = rng.choice(meta["subcategories"], size=k)
        s = np.empty(n, dtype=object)
        # assign back
        price[mask] = _lognormal_prices(rng, meta["price_band"], k)
        wlo, whi = meta["weight_grams"]
        weight_g[mask] = rng.integers(wlo, whi + 1, size=k)
        # subcategory bookkeeping
        tmp = np.array(subcat_arr, dtype=object)
        idxs = np.where(mask)[0]
        for j, ii in enumerate(idxs):
            subcat.append((ii, tmp[j]))
    subcat_sorted = [s for _, s in sorted(subcat)]
    return pd.DataFrame({
        "product_id": [f"PROD{i:07d}" for i in range(n)],
        "product_name": [f"{rng.choice(ref.BRANDS)} {sc}" for sc in subcat_sorted],
        "category": category,
        "subcategory": subcat_sorted,
        "brand": rng.choice(ref.BRANDS, size=n),
        "mrp_inr": price,
        "weight_grams": weight_g.astype(int),
    })


# --------------------------------------------------------------------------- #
# Fact-like source entities
# --------------------------------------------------------------------------- #
def gen_orders_and_children(rng, cfg, customers, sellers, products):
    n = cfg.n_orders

    # ---- repeat-customer behaviour: Zipf over customer index -------------- #
    ncust = len(customers)
    ranks = rng.zipf(cfg.repeat_customer_zipf_a, size=n) % ncust
    cust_ids = customers["customer_id"].to_numpy()[ranks]
    cust_city = customers["city"].to_numpy()[ranks]
    cust_state = customers["state"].to_numpy()[ranks]

    order_ids = np.array([f"ORD{i:09d}" for i in range(n)])
    purchase_ts = _make_dates(rng, cfg, n)

    status = rng.choice(ref.ORDER_STATUSES, size=n, p=ref.ORDER_STATUS_WEIGHTS)
    ptype = rng.choice(ref.PAYMENT_TYPES, size=n, p=ref.PAYMENT_TYPE_WEIGHTS)

    orders = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": cust_ids,
        "order_status": status,
        "order_purchase_ts": purchase_ts,
        "order_approved_ts": purchase_ts + pd.to_timedelta(rng.integers(5, 720, n), unit="m"),
        "payment_type": ptype,
    })

    # ---- order_items: 1..max items per order (right-skewed) --------------- #
    item_counts = rng.choice(
        np.arange(1, cfg.max_items_per_order + 1),
        size=n, p=_skewed_probs(cfg.max_items_per_order),
    )
    total_items = int(item_counts.sum())
    oi_order_id = np.repeat(order_ids, item_counts)
    oi_seq = np.concatenate([np.arange(1, c + 1) for c in item_counts])

    prod_idx = rng.integers(0, len(products), size=total_items)
    sell_idx = rng.integers(0, len(sellers), size=total_items)
    unit_price = products["mrp_inr"].to_numpy()[prod_idx]
    # discount 0-40% of MRP, freight scales with weight + a floor
    discount_pct = rng.choice([0, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4],
                              size=total_items, p=[0.35, 0.15, 0.15, 0.12, 0.1, 0.08, 0.05])
    unit_price_net = (unit_price * (1 - discount_pct)).round(0)
    qty = rng.choice([1, 1, 1, 2, 2, 3], size=total_items)
    weight_g = products["weight_grams"].to_numpy()[prod_idx]
    freight = (40 + weight_g / 1000.0 * 18 + rng.normal(0, 8, total_items)).clip(0).round(0)

    order_items = pd.DataFrame({
        "order_id": oi_order_id,
        "order_item_id": oi_seq,
        "product_id": products["product_id"].to_numpy()[prod_idx],
        "seller_id": sellers["seller_id"].to_numpy()[sell_idx],
        "quantity": qty,
        "unit_price_inr": unit_price_net,
        "discount_inr": (unit_price * discount_pct).round(0),
        "freight_inr": freight,
    })
    order_items["line_total_inr"] = (
        order_items["unit_price_inr"] * order_items["quantity"] + order_items["freight_inr"]
    ).round(0)

    # ---- payments: aggregate to order grain, sometimes installments ------- #
    order_value = order_items.groupby("order_id", sort=False)["line_total_inr"].sum()
    order_value = order_value.reindex(order_ids).fillna(0.0)
    installments = np.where(
        (orders["payment_type"].isin(["Credit Card"])) & (order_value.to_numpy() > 5000),
        rng.choice([1, 3, 6, 9], size=n, p=[0.4, 0.3, 0.2, 0.1]),
        1,
    )
    pay_status = np.where(
        status == "cancelled", "failed",
        np.where(status == "returned", "refunded", "paid"),
    )
    payments = pd.DataFrame({
        "order_id": order_ids,
        "payment_sequential": 1,
        "payment_type": orders["payment_type"].to_numpy(),
        "payment_installments": installments,
        "payment_value_inr": order_value.to_numpy().round(0),
        "payment_status": pay_status,
    })

    # ---- shipments: only for orders that actually move -------------------- #
    moves = np.isin(status, ["shipped", "delivered", "returned"])
    ship_orders = order_ids[moves]
    ship_purchase = purchase_ts[moves]
    # destination tier from customer city
    dest_tier = _tier_for_cities(cust_city[moves])
    base_days = np.select(
        [dest_tier == 1, dest_tier == 2, dest_tier == 3],
        [2, 4, 7], default=4,
    )
    est_days = base_days + rng.integers(1, 3, size=moves.sum())
    actual_days = (base_days + rng.poisson(1.2, size=moves.sum())
                   + rng.integers(-1, 3, size=moves.sum())).clip(1, None)
    ship_ts = ship_purchase + pd.to_timedelta(rng.integers(6, 48, moves.sum()), unit="h")
    est_delivery = (pd.to_datetime(ship_purchase).normalize()
                    + pd.to_timedelta(est_days, unit="D"))
    delivered_ts = ship_ts + pd.to_timedelta(actual_days, unit="D")
    is_delivered = np.isin(status[moves], ["delivered", "returned"])
    delivered_final = np.where(is_delivered, delivered_ts, pd.NaT)
    shipments = pd.DataFrame({
        "order_id": ship_orders,
        "carrier": rng.choice(ref.CARRIERS, size=moves.sum()),
        "ship_ts": ship_ts,
        "estimated_delivery_date": est_delivery.date,
        "delivered_ts": pd.to_datetime(delivered_final),
        "delivery_status": np.where(is_delivered, "delivered", "in_transit"),
    })

    # ---- returns: subset of items from 'returned' orders ------------------ #
    returned_orders = set(order_ids[status == "returned"])
    ret_mask = order_items["order_id"].isin(returned_orders).to_numpy()
    cand = order_items[ret_mask].copy()
    # category-driven probability of a given line being the returned one
    prod_cat = products.set_index("product_id")["category"]
    cand_cat = cand["product_id"].map(prod_cat)
    cand_rate = cand_cat.map(lambda c: ref.CATEGORIES.get(c, {}).get("return_rate", 0.06))
    keep = rng.random(len(cand)) < (0.5 + cand_rate.to_numpy())  # ensure most returned orders yield >=1
    returns_src = cand[keep].copy()
    nret = len(returns_src)
    ret_delivered = shipments.set_index("order_id")["delivered_ts"]
    base_deliv = returns_src["order_id"].map(ret_delivered)
    base_deliv = pd.to_datetime(base_deliv).fillna(pd.Timestamp(cfg.end_date))
    req_ts = base_deliv + pd.to_timedelta(rng.integers(1, 12, nret), unit="D")
    returns = pd.DataFrame({
        "return_id": [f"RET{i:08d}" for i in range(nret)],
        "order_id": returns_src["order_id"].to_numpy(),
        "order_item_id": returns_src["order_item_id"].to_numpy(),
        "product_id": returns_src["product_id"].to_numpy(),
        "return_reason": rng.choice(ref.RETURN_REASONS, size=nret),
        "return_status": rng.choice(["requested", "approved", "completed", "rejected"],
                                    size=nret, p=[0.1, 0.2, 0.6, 0.1]),
        "return_qty": returns_src["quantity"].to_numpy(),
        "refund_amount_inr": (returns_src["unit_price_inr"] * returns_src["quantity"]).to_numpy().round(0),
        "return_requested_ts": req_ts,
        "return_completed_ts": req_ts + pd.to_timedelta(rng.integers(2, 15, nret), unit="D"),
    })

    return orders, order_items, payments, shipments, returns


def _skewed_probs(k: int) -> np.ndarray:
    base = np.array([1 / (i ** 1.8) for i in range(1, k + 1)])
    return base / base.sum()


_TIER_LOOKUP = {c[0]: c[2] for c in ref.CITIES}


def _tier_for_cities(cities: np.ndarray) -> np.ndarray:
    return np.array([_TIER_LOOKUP.get(c, 2) for c in cities])


# --------------------------------------------------------------------------- #
# Deliberate data-quality defects
# --------------------------------------------------------------------------- #
def inject_defects(rng, cfg, customers, sellers, products, orders,
                   order_items, payments, shipments, returns):
    log.info("Injecting deliberate data-quality defects ...")

    # 1. Dirty city names in customers (alias / casing / whitespace)
    m = rng.random(len(customers)) < cfg.dq_dirty_city_names
    for i in np.where(m)[0]:
        canon = customers.iat[i, customers.columns.get_loc("city")]
        aliases = ref.CITY_ALIASES.get(canon)
        if aliases:
            customers.iat[i, customers.columns.get_loc("city")] = rng.choice(aliases)

    # 2. Null city/state in customers
    m = rng.random(len(customers)) < cfg.dq_null_city
    customers.loc[m, "city"] = np.nan

    # 3. Duplicate customers (append copies with same customer_id)
    ndup = int(len(customers) * cfg.dq_duplicate_customers)
    if ndup:
        dup = customers.sample(ndup, random_state=int(rng.integers(1e9)))
        customers = pd.concat([customers, dup], ignore_index=True)

    # 4. Dirty categories in products
    m = rng.random(len(products)) < cfg.dq_dirty_categories
    for i in np.where(m)[0]:
        canon = products.iat[i, products.columns.get_loc("category")]
        products.iat[i, products.columns.get_loc("category")] = rng.choice(ref.category_variants(canon))

    # 5. Negative prices in products
    m = rng.random(len(products)) < cfg.dq_negative_price
    products.loc[m, "mrp_inr"] = -products.loc[m, "mrp_inr"]

    # 6. Duplicate order rows (same order_id)
    ndup = int(len(orders) * cfg.dq_duplicate_orders)
    if ndup:
        dup = orders.sample(ndup, random_state=int(rng.integers(1e9)))
        orders = pd.concat([orders, dup], ignore_index=True)

    # 7. Invalid dates: delivered before purchase + a few future dates
    if len(shipments):
        m = rng.random(len(shipments)) < cfg.dq_invalid_dates
        bad_idx = np.where(m)[0]
        for i in bad_idx:
            # delivered_ts far in the past relative to ship
            shipments.iat[i, shipments.columns.get_loc("delivered_ts")] = (
                shipments.iat[i, shipments.columns.get_loc("ship_ts")] - pd.Timedelta(days=3)
            )
        # a handful of impossible future dates
        fut = rng.random(len(shipments)) < (cfg.dq_invalid_dates / 3)
        shipments.loc[fut, "delivered_ts"] = pd.Timestamp("2099-01-01")

    # 8. Null delivered_ts even where delivered
    m = (rng.random(len(shipments)) < cfg.dq_null_delivered_ts)
    shipments.loc[m, "delivered_ts"] = pd.NaT

    # 9. Bad quantities (zero / negative)
    m = rng.random(len(order_items)) < cfg.dq_bad_quantity
    order_items.loc[m, "quantity"] = rng.choice([0, -1, -2], size=int(m.sum()))

    # 10. Orphan order_items (order_id that does not exist)
    norph = int(len(order_items) * cfg.dq_orphan_order_items)
    if norph:
        orphan = order_items.sample(norph, random_state=int(rng.integers(1e9))).copy()
        orphan["order_id"] = [f"ORD999{ i:06d}" for i in range(norph)]
        order_items = pd.concat([order_items, orphan], ignore_index=True)

    # 11. Null payment values
    m = rng.random(len(payments)) < cfg.dq_null_payment_value
    payments.loc[m, "payment_value_inr"] = np.nan

    # 12. Dirty payment statuses (map canonical -> messy variants)
    m = rng.random(len(payments)) < cfg.dq_dirty_payment_status
    for i in np.where(m)[0]:
        canon = payments.iat[i, payments.columns.get_loc("payment_status")]
        variants = ref.PAYMENT_STATUS_DIRTY.get(canon)
        if variants:
            payments.iat[i, payments.columns.get_loc("payment_status")] = rng.choice(variants)

    return customers, sellers, products, orders, order_items, payments, shipments, returns


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def generate(cfg: GeneratorConfig) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(cfg.seed)
    log.info("Generating customers=%d sellers=%d products=%d orders=%d",
             cfg.n_customers, cfg.n_sellers, cfg.n_products, cfg.n_orders)

    customers = gen_customers(rng, cfg)
    sellers = gen_sellers(rng, cfg)
    products = gen_products(rng, cfg)
    orders, order_items, payments, shipments, returns = gen_orders_and_children(
        rng, cfg, customers, sellers, products
    )
    (customers, sellers, products, orders, order_items,
     payments, shipments, returns) = inject_defects(
        rng, cfg, customers, sellers, products, orders,
        order_items, payments, shipments, returns
    )

    return {
        "customers": customers,
        "sellers": sellers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "shipments": shipments,
        "returns": returns,
    }


def write_csv(tables: dict[str, pd.DataFrame], out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        path = out / f"{name}.csv"
        df.to_csv(path, index=False)
        log.info("wrote %-12s %8d rows -> %s", name, len(df), path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic Indian e-commerce data")
    ap.add_argument("--orders", type=int, help="number of orders (overrides config)")
    ap.add_argument("--scale", type=float, help="scale full config by this factor (e.g. 0.015)")
    ap.add_argument("--output", type=str, default=None, help="output directory")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--config", type=str, default="configs/data_generation.yml")
    args = ap.parse_args()

    cfg = load_yaml_overrides(args.config, GeneratorConfig())
    if args.scale:
        cfg = cfg.scaled(args.scale)
    if args.orders:
        # rescale entities proportionally to keep ratios sane
        factor = args.orders / cfg.n_orders
        cfg = cfg.scaled(factor) if factor < 1 else cfg
        cfg.n_orders = args.orders
    if args.output:
        cfg.output_dir = args.output
    if args.seed is not None:
        cfg.seed = args.seed

    t0 = time.perf_counter()
    tables = generate(cfg)
    write_csv(tables, cfg.output_dir)
    log.info("done in %.1fs", time.perf_counter() - t0)


if __name__ == "__main__":
    main()
