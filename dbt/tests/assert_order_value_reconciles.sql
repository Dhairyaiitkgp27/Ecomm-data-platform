-- Singular data test: an order's stored order_value should match the sum of its
-- item line totals within a small rounding tolerance. Returns offending rows;
-- dbt fails the test if any are returned.
with order_totals as (
    select order_id, sum(line_total_inr) as items_total
    from {{ ref('fact_order_items') }}
    group by order_id
),
orders as (
    select order_id, order_value_inr from {{ ref('fact_orders') }}
)
select
    o.order_id,
    o.order_value_inr,
    t.items_total,
    abs(o.order_value_inr - t.items_total) as diff
from orders o
join order_totals t on o.order_id = t.order_id
where abs(o.order_value_inr - t.items_total) > 1.0
