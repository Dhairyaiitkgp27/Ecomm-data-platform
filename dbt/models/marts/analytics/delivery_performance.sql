{#
  GRAIN: one row per (state, carrier). Average delivery days and on-time rate
  for the delivery-SLA view.
#}
with s as (
    select
        f.order_id, f.carrier, f.delivery_days, f.is_late,
        c.state
    from {{ ref('fact_shipments') }} f
    join {{ ref('fact_orders') }} o on f.order_id = o.order_id
    join {{ ref('dim_customer') }} c on o.customer_id = c.customer_id
    where f.delivery_days is not null
)
select
    state,
    carrier,
    count(*)                                         as shipments,
    round(avg(delivery_days)::numeric, 2)                     as avg_delivery_days,
    sum(is_late)                                     as late_deliveries,
    {{ safe_divide('sum(is_late)', 'count(*)') }}    as late_rate,
    1 - {{ safe_divide('sum(is_late)', 'count(*)') }} as on_time_rate
from s
group by state, carrier
