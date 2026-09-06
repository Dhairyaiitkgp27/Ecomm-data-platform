-- ============================================================================
--  E-commerce warehouse: business analytics queries
--  Run against the dbt-built marts (schemas: marts.*, analytics.*).
--  Every query uses CTEs / window functions / joins — no trivial SELECT *.
-- ============================================================================


-- 1. Monthly GMV with month-over-month growth -------------------------------
with monthly as (
    select d.year, d.month,
           sum(f.gmv_inr) as gmv_inr,
           count(*) filter (where f.is_cancelled = 0) as orders
    from marts.fact_orders f
    join marts.dim_date d on f.order_date_key = d.date_key
    where f.is_cancelled = 0
    group by d.year, d.month
)
select
    year, month, gmv_inr, orders,
    lag(gmv_inr) over (order by year, month) as prev_month_gmv,
    round((100.0 * (gmv_inr - lag(gmv_inr) over (order by year, month))
          / nullif(lag(gmv_inr) over (order by year, month), 0))::numeric, 2) as mom_growth_pct
from monthly
order by year, month;


-- 2. Daily orders and 7-day moving average ----------------------------------
select
    order_date,
    orders,
    round(avg(orders) over (order by order_date
          rows between 6 preceding and current row), 1) as orders_7d_avg
from analytics.daily_sales
order by order_date;


-- 3. Average Order Value (AOV) — overall and by month -----------------------
with base as (
    select d.year, d.month, f.order_value_inr
    from marts.fact_orders f
    join marts.dim_date d on f.order_date_key = d.date_key
    where f.is_cancelled = 0
)
select
    year, month,
    count(*) as orders,
    round((sum(order_value_inr) / nullif(count(*), 0))::numeric, 2) as aov_inr
from base
group by rollup (year, month)
order by year nulls last, month nulls last;


-- 4. Top 20 products by GMV -------------------------------------------------
select
    product_id, product_name, category, brand,
    units_sold, gmv_inr,
    rank() over (order by gmv_inr desc) as gmv_rank
from analytics.product_performance
order by gmv_inr desc
limit 20;


-- 5. Top 10 sellers by GMV with their share of total ------------------------
with s as (
    select seller_id, seller_name, seller_state, gmv_inr,
           sum(gmv_inr) over () as total_gmv
    from analytics.seller_performance
)
select
    seller_id, seller_name, seller_state, gmv_inr,
    round((100.0 * gmv_inr / nullif(total_gmv, 0))::numeric, 2) as pct_of_total_gmv,
    dense_rank() over (order by gmv_inr desc) as seller_rank
from s
order by gmv_inr desc
limit 10;


-- 6. Repeat customers: how many, and their share of revenue -----------------
select
    count(*)                                              as customers,
    count(*) filter (where is_repeat_customer)            as repeat_customers,
    round(100.0 * count(*) filter (where is_repeat_customer)
          / nullif(count(*), 0), 2)                       as repeat_customer_pct,
    sum(total_spend_inr)                                  as total_revenue,
    round((100.0 * sum(total_spend_inr) filter (where is_repeat_customer)
          / nullif(sum(total_spend_inr), 0))::numeric, 2) as pct_revenue_from_repeat
from analytics.customer_summary;


-- 7. Monthly acquisition cohort retention (simplified) ----------------------
-- For each signup-month cohort, how many placed an order in each later month.
with cust_cohort as (
    select customer_id, date_trunc('month', signup_date)::date as cohort_month
    from marts.dim_customer
),
orders as (
    select o.customer_id,
           date_trunc('month', d.full_date)::date as order_month
    from marts.fact_orders o
    join marts.dim_date d on o.order_date_key = d.date_key
    where o.is_cancelled = 0
)
select
    c.cohort_month,
    o.order_month,
    (extract(year from age(o.order_month, c.cohort_month)) * 12
     + extract(month from age(o.order_month, c.cohort_month)))::int as month_number,
    count(distinct o.customer_id) as active_customers
from cust_cohort c
join orders o using (customer_id)
where o.order_month >= c.cohort_month
group by c.cohort_month, o.order_month
order by c.cohort_month, o.order_month;


-- 8. Cancellation rate overall and by month ---------------------------------
select
    d.year, d.month,
    count(*)                                        as orders,
    sum(f.is_cancelled)                             as cancelled,
    round(100.0 * sum(f.is_cancelled) / nullif(count(*), 0), 2) as cancellation_rate_pct
from marts.fact_orders f
join marts.dim_date d on f.order_date_key = d.date_key
group by grouping sets ((d.year, d.month), ())
order by d.year nulls last, d.month nulls last;


-- 9. Return rate by category ------------------------------------------------
with sold as (
    select category, sum(quantity) as units_sold
    from marts.fact_order_items
    group by category
),
returned as (
    select oi.category, sum(r.return_qty) as units_returned
    from marts.fact_returns r
    join marts.fact_order_items oi
      on r.order_id = oi.order_id and r.order_item_id = oi.order_item_id
    group by oi.category
)
select
    s.category,
    s.units_sold,
    coalesce(rt.units_returned, 0) as units_returned,
    round(100.0 * coalesce(rt.units_returned, 0) / nullif(s.units_sold, 0), 2) as return_rate_pct
from sold s
left join returned rt on s.category = rt.category
order by return_rate_pct desc;


-- 10. COD vs prepaid performance --------------------------------------------
select
    case when is_cod = 1 then 'COD' else 'Prepaid' end as payment_mode,
    count(*)                                            as orders,
    round(sum(gmv_inr))                                 as gmv_inr,
    round((sum(order_value_inr) / nullif(count(*), 0))::numeric, 2) as aov_inr,
    round(100.0 * sum(is_cancelled) / nullif(count(*), 0), 2) as cancellation_rate_pct,
    round(100.0 * sum(is_returned) / nullif(count(*), 0), 2)  as return_rate_pct
from marts.fact_orders
group by (is_cod = 1)
order by payment_mode;


-- 11. State-wise sales (top 15 states) --------------------------------------
select
    c.state,
    count(*)                                        as orders,
    round(sum(f.gmv_inr))                           as gmv_inr,
    round((sum(f.order_value_inr) / nullif(count(*), 0))::numeric, 2) as aov_inr,
    dense_rank() over (order by sum(f.gmv_inr) desc) as state_rank
from marts.fact_orders f
join marts.dim_customer c on f.customer_id = c.customer_id
where f.is_cancelled = 0
group by c.state
order by gmv_inr desc
limit 15;


-- 12. Delivery SLA performance by carrier -----------------------------------
select
    carrier,
    sum(shipments)                                   as shipments,
    round(sum(avg_delivery_days * shipments) / nullif(sum(shipments), 0), 2) as avg_delivery_days,
    round((100.0 * sum(late_deliveries) / nullif(sum(shipments), 0))::numeric, 2)       as late_rate_pct,
    round(100.0 * (1 - sum(late_deliveries)::numeric / nullif(sum(shipments), 0)), 2) as on_time_rate_pct
from analytics.delivery_performance
group by carrier
order by on_time_rate_pct desc;


-- 13. Products with increasing return rate (H2 vs H1) -----------------------
with lines as (
    select oi.product_id, d.full_date, oi.quantity,
           (r.return_id is not null)::int as is_returned
    from marts.fact_order_items oi
    join marts.dim_date d on oi.order_date_key = d.date_key
    left join marts.fact_returns r
           on oi.order_id = r.order_id and oi.order_item_id = r.order_item_id
),
periods as (
    select product_id,
           case when full_date < date '2024-01-01' then 'H1' else 'H2' end as period,
           sum(is_returned)::numeric / nullif(sum(quantity), 0) as return_rate
    from lines
    group by product_id, case when full_date < date '2024-01-01' then 'H1' else 'H2' end
),
pivot as (
    select product_id,
           max(return_rate) filter (where period = 'H1') as h1_rate,
           max(return_rate) filter (where period = 'H2') as h2_rate
    from periods
    group by product_id
)
select p.product_id, pr.product_name, pr.category,
       round(h1_rate, 3) as h1_return_rate,
       round(h2_rate, 3) as h2_return_rate,
       round(h2_rate - h1_rate, 3) as delta
from pivot p
join marts.dim_product pr on p.product_id = pr.product_id
where h1_rate is not null and h2_rate is not null and h2_rate > h1_rate
order by delta desc
limit 25;


-- 14. Seller ranking within their state -------------------------------------
select seller_state, seller_id, seller_name, gmv_inr, rank_in_state
from (
    select seller_state, seller_id, seller_name, gmv_inr,
           dense_rank() over (partition by seller_state order by gmv_inr desc) as rank_in_state
    from analytics.seller_performance
    where gmv_inr > 0
) ranked
order by seller_state, rank_in_state;


-- 15. New vs repeat order revenue split by month ----------------------------
select
    d.year, d.month,
    sum(f.gmv_inr) filter (where f.is_repeat_order = 0) as new_customer_gmv,
    sum(f.gmv_inr) filter (where f.is_repeat_order = 1) as repeat_customer_gmv,
    round((100.0 * sum(f.gmv_inr) filter (where f.is_repeat_order = 1)
          / nullif(sum(f.gmv_inr), 0))::numeric, 2) as repeat_gmv_pct
from marts.fact_orders f
join marts.dim_date d on f.order_date_key = d.date_key
where f.is_cancelled = 0
group by d.year, d.month
order by d.year, d.month;


-- 16. Top category by GMV each month (window rank) --------------------------
with cat_month as (
    select d.year, d.month, oi.category, sum(oi.line_total_inr) as gmv_inr
    from marts.fact_order_items oi
    join marts.dim_date d on oi.order_date_key = d.date_key
    group by d.year, d.month, oi.category
)
select year, month, category, round(gmv_inr) as gmv_inr
from (
    select *, row_number() over (partition by year, month order by gmv_inr desc) as rn
    from cat_month
) t
where rn = 1
order by year, month;


-- 17. Average delivery days: metro vs non-metro -----------------------------
select
    l.is_metro,
    count(*)                              as shipments,
    round(avg(s.delivery_days)::numeric, 2)        as avg_delivery_days,
    round((100.0 * avg(s.is_late))::numeric, 2)      as late_rate_pct
from marts.fact_shipments s
join marts.fact_orders o  on s.order_id = o.order_id
join marts.dim_customer c on o.customer_id = c.customer_id
join marts.dim_location l on l.city = c.city and l.state = c.state
where s.delivery_days is not null
group by l.is_metro;
