{#
  GRAIN: one row per seller. GMV, orders, units, return rate and on-time rate,
  joined to current seller attributes. Powers the "top sellers" view.
#}
with items as (
    select seller_id,
           count(distinct order_id)   as orders,
           sum(quantity)              as units,
           sum(line_total_inr)        as gmv_inr
    from {{ ref('fact_order_items') }}
    group by seller_id
),
returns as (
    select oi.seller_id, count(*) as returned_lines
    from {{ ref('fact_returns') }} r
    join {{ ref('fact_order_items') }} oi
      on r.order_id = oi.order_id and r.order_item_id = oi.order_item_id
    group by oi.seller_id
)
select
    s.seller_id,
    s.seller_name,
    s.seller_state,
    s.seller_tier,
    s.seller_rating,
    s.business_type,
    coalesce(i.orders, 0)   as orders,
    coalesce(i.units, 0)    as units,
    coalesce(i.gmv_inr, 0)  as gmv_inr,
    coalesce(r.returned_lines, 0) as returned_lines,
    {{ safe_divide('coalesce(r.returned_lines,0)', 'nullif(i.units,0)') }} as return_rate,
    round({{ safe_divide('coalesce(i.gmv_inr,0)', 'nullif(i.orders,0)') }}, 2) as avg_order_value_inr
from {{ ref('dim_seller') }} s
left join items i   on s.seller_id = i.seller_id
left join returns r on s.seller_id = r.seller_id
