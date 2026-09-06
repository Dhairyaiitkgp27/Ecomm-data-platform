"""
Streamlit dashboard for the e-commerce warehouse.

Two data sources (toggle in the sidebar):
  * Warehouse  — reads the dbt marts from PostgreSQL (the real deliverable).
  * Sample CSV — computes headline KPIs directly from data/sample so the
                 dashboard is viewable before the full pipeline has run.

The dashboard is intentionally secondary to the data engineering; it surfaces
GMV, orders, AOV, cancellation/return rates, top categories & sellers,
state-wise sales, delivery performance and the monthly trend.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

st.set_page_config(page_title="E-Commerce Analytics", layout="wide")


# --------------------------------------------------------------------------- #
# Data access
# --------------------------------------------------------------------------- #
@st.cache_resource
def _engine():
    from sqlalchemy import create_engine

    user = os.environ.get("POSTGRES_USER", "warehouse")
    pw = os.environ.get("POSTGRES_PASSWORD", "warehouse")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ecommerce")
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, _engine())


@st.cache_data
def sample_frames(path: str = "data/sample") -> dict[str, pd.DataFrame]:
    out = {}
    for name in ["orders", "order_items", "products", "customers", "shipments", "returns"]:
        p = os.path.join(path, f"{name}.csv")
        if os.path.exists(p):
            out[name] = pd.read_csv(p)
    return out


# --------------------------------------------------------------------------- #
# KPI computation
# --------------------------------------------------------------------------- #
def kpis_from_warehouse():
    head = q("""
        select
            sum(gmv_inr)  as gmv,
            count(*) filter (where is_cancelled = 0) as orders,
            sum(order_value_inr) filter (where is_cancelled = 0)
                / nullif(count(*) filter (where is_cancelled = 0),0) as aov,
            avg(is_cancelled::float) as cancel_rate,
            avg(is_returned::float)  as return_rate
        from marts.fact_orders
    """).iloc[0]
    trend = q("""
        select date_trunc('month', order_date)::date as month,
               sum(gmv_inr) as gmv, sum(orders) as orders
        from analytics.daily_sales group by 1 order by 1
    """)
    top_cat = q("""
        select category, sum(gmv_inr) as gmv
        from marts.fact_order_items group by 1 order by 2 desc limit 10
    """)
    top_sellers = q("""
        select seller_name, gmv_inr as gmv from analytics.seller_performance
        order by gmv_inr desc limit 10
    """)
    states = q("""
        select c.state, sum(f.gmv_inr) as gmv
        from marts.fact_orders f join marts.dim_customer c using (customer_id)
        where f.is_cancelled = 0 group by 1 order by 2 desc limit 15
    """)
    delivery = q("""
        select carrier,
               sum(avg_delivery_days*shipments)/nullif(sum(shipments),0) as avg_days,
               100.0*sum(late_deliveries)/nullif(sum(shipments),0) as late_pct
        from analytics.delivery_performance group by 1 order by 2
    """)
    return head, trend, top_cat, top_sellers, states, delivery


def kpis_from_sample(f: dict[str, pd.DataFrame]):
    orders = f["orders"].drop_duplicates("order_id")
    items = f["order_items"]
    items = items[items["quantity"] > 0]
    prod = f["products"][["product_id", "category"]].copy()
    prod["category"] = prod["category"].astype(str).str.strip().str.title()
    items = items.merge(prod, on="product_id", how="left")
    items["line_total_inr"] = items["unit_price_inr"] * items["quantity"] + items["freight_inr"].fillna(0)

    valid = orders[orders["order_status"].str.lower() != "cancelled"]
    order_gmv = items.groupby("order_id")["line_total_inr"].sum()
    gmv = order_gmv.reindex(valid["order_id"]).sum()
    n_orders = len(valid)
    aov = gmv / max(n_orders, 1)
    cancel_rate = (orders["order_status"].str.lower() == "cancelled").mean()
    return_rate = (orders["order_status"].str.lower() == "returned").mean()

    orders["month"] = pd.to_datetime(orders["order_purchase_ts"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    o2 = orders.merge(order_gmv.rename("gmv"), on="order_id", how="left")
    trend = o2.groupby("month").agg(gmv=("gmv", "sum"), orders=("order_id", "nunique")).reset_index()

    top_cat = items.groupby("category")["line_total_inr"].sum().sort_values(ascending=False).head(10).reset_index()
    top_cat.columns = ["category", "gmv"]

    head = pd.Series({"gmv": gmv, "orders": n_orders, "aov": aov,
                      "cancel_rate": cancel_rate, "return_rate": return_rate})
    return head, trend, top_cat


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
st.title("🛒 E-Commerce Analytics — India")
source = st.sidebar.radio("Data source", ["Warehouse (Postgres)", "Sample CSV"])
st.sidebar.caption("Warehouse = dbt marts. Sample CSV = quick preview from data/sample.")

try:
    if source.startswith("Warehouse"):
        head, trend, top_cat, top_sellers, states, delivery = kpis_from_warehouse()
        full = True
    else:
        frames = sample_frames()
        if not frames:
            st.warning("No sample data found. Run `make generate-sample` first.")
            st.stop()
        head, trend, top_cat = kpis_from_sample(frames)
        top_sellers = states = delivery = None
        full = False
except Exception as e:  # noqa: BLE001
    st.error(f"Could not load data from the warehouse ({e}). "
             f"Switch to 'Sample CSV' in the sidebar, or run the pipeline first.")
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("GMV", f"₹{head['gmv']/1e5:,.1f} L")
c2.metric("Orders", f"{int(head['orders']):,}")
c3.metric("AOV", f"₹{head['aov']:,.0f}")
c4.metric("Cancellation", f"{head['cancel_rate']*100:.1f}%")
c5.metric("Return rate", f"{head['return_rate']*100:.1f}%")

st.subheader("Monthly GMV trend")
st.line_chart(trend.set_index("month")[["gmv"]])
st.subheader("Monthly orders")
st.bar_chart(trend.set_index("month")[["orders"]])

left, right = st.columns(2)
with left:
    st.subheader("Top categories by GMV")
    st.bar_chart(top_cat.set_index("category")["gmv"])
with right:
    if full:
        st.subheader("Top sellers by GMV")
        st.dataframe(top_sellers, use_container_width=True, hide_index=True)
    else:
        st.info("Seller / state / delivery views require the warehouse. "
                "Run the full pipeline to enable them.")

if full:
    a, b = st.columns(2)
    with a:
        st.subheader("State-wise sales (top 15)")
        st.bar_chart(states.set_index("state")["gmv"])
    with b:
        st.subheader("Delivery performance by carrier")
        st.dataframe(delivery.round(2), use_container_width=True, hide_index=True)

st.caption("Synthetic Indian e-commerce data — see docs/ for the data-generation methodology.")
