{#
  Aggregate order_items to order grain: GMV, units, freight, discount, distinct
  products/sellers. Feeds fact_orders and several analytics marts.
#}
with items as (
    select * from {{ ref('stg_order_items') }}
)
select
    order_id,
    count(*)                                as n_items,
    sum(quantity)                           as total_units,
    count(distinct product_id)              as n_distinct_products,
    count(distinct seller_id)               as n_distinct_sellers,
    sum(unit_price_inr * quantity)          as gross_merchandise_value_inr,
    sum(discount_inr)                       as total_discount_inr,
    sum(freight_inr)                        as total_freight_inr,
    sum(line_total_inr)                     as order_line_total_inr
from items
group by order_id
