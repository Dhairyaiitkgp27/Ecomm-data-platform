{#
  GRAIN: one row per product. Units sold, GMV and return rate; used for
  best-sellers and "products with rising returns".
#}
with items as (
    select product_id,
           sum(quantity)       as units_sold,
           sum(line_total_inr) as gmv_inr,
           count(distinct order_id) as orders
    from {{ ref('fact_order_items') }}
    group by product_id
),
returns as (
    select product_id, sum(return_qty) as units_returned
    from {{ ref('fact_returns') }}
    group by product_id
)
select
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.mrp_inr,
    coalesce(i.units_sold, 0)      as units_sold,
    coalesce(i.orders, 0)          as orders,
    coalesce(i.gmv_inr, 0)         as gmv_inr,
    coalesce(r.units_returned, 0)  as units_returned,
    {{ safe_divide('coalesce(r.units_returned,0)', 'nullif(i.units_sold,0)') }} as return_rate
from {{ ref('dim_product') }} p
left join items i   on p.product_id = i.product_id
left join returns r on p.product_id = r.product_id
