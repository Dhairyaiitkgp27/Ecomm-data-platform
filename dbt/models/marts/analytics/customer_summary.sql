{#
  GRAIN: one row per customer. Lifetime orders, spend, AOV, first/last order and
  a repeat flag. Powers retention / repeat-customer analysis.
#}
with o as (
    select
        customer_id,
        count(*) filter (where is_cancelled = 0)     as orders,
        sum(gmv_inr) filter (where is_cancelled = 0)  as total_spend_inr,
        min(order_date_key)                          as first_order_date_key,
        max(order_date_key)                          as last_order_date_key
    from {{ ref('fact_orders') }}
    group by customer_id
)
select
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    c.customer_segment,
    coalesce(o.orders, 0)                                   as orders,
    coalesce(o.total_spend_inr, 0)                          as total_spend_inr,
    round({{ safe_divide('coalesce(o.total_spend_inr,0)', 'nullif(o.orders,0)') }}, 2) as aov_inr,
    o.first_order_date_key,
    o.last_order_date_key,
    (coalesce(o.orders, 0) > 1)                             as is_repeat_customer
from {{ ref('dim_customer') }} c
left join o on c.customer_id = o.customer_id
