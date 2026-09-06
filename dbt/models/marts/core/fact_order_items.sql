{#
  GRAIN: one row per product line within an order (order_id + order_item_id).
  This is the finest-grain, largest fact and the INCREMENTAL model: on an
  incremental run it only processes rows whose order_date is on/after the most
  recent order_date already in the table (minus a 3-day look-back to absorb
  late-arriving lines), then merges on the composite key.
#}
{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
  )
}}
with items as (
    select * from {{ ref('stg_order_items') }}
),
orders as (
    select order_id, customer_id, order_date, order_status from {{ ref('stg_orders') }}
)
select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    {{ date_key('o.order_date') }}     as order_date_key,
    o.order_date,
    o.order_status,
    i.quantity,
    i.unit_price_inr,
    i.discount_inr,
    i.freight_inr,
    i.line_total_inr,
    (i.unit_price_inr * i.quantity)    as gross_line_value_inr,
    i.category,
    i.brand
from items i
join orders o on i.order_id = o.order_id
{% if is_incremental() %}
  where o.order_date >= (select coalesce(max(order_date), '1900-01-01') from {{ this }}) - interval '3 days'
{% endif %}
