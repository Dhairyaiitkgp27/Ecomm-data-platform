{#
  GRAIN: one row per order.
  Order-level fact combining item aggregates, payment rollup and shipment SLA.
  FKs: customer_id -> dim_customer, order_date_key -> dim_date,
       location_key -> dim_location (customer geography).
  Degenerate dimensions: order_id, order_status, payment_type.
#}
with orders as (
    select * from {{ ref('stg_orders') }}
),
items as (
    select * from {{ ref('int_order_item_metrics') }}
),
pay as (
    select * from {{ ref('int_order_payments') }}
),
ship as (
    select order_id, delivery_days, is_late, delivered_ts
    from {{ ref('stg_shipments') }}
),
cust as (
    select customer_id, city, state from {{ ref('stg_customers') }}
)
select
    o.order_id,
    o.customer_id,
    {{ date_key('o.order_date') }}                      as order_date_key,
    {{ dbt_utils.generate_surrogate_key(['cust.city', 'cust.state']) }} as location_key,
    o.order_status,
    o.payment_type,
    o.is_cod,
    o.is_repeat_order,
    coalesce(i.gross_merchandise_value_inr, 0)         as gmv_inr,
    coalesce(i.order_line_total_inr, 0)                as order_value_inr,
    coalesce(i.total_units, 0)                         as total_units,
    coalesce(i.n_items, 0)                             as n_items,
    coalesce(i.total_discount_inr, 0)                  as discount_inr,
    coalesce(i.total_freight_inr, 0)                   as freight_inr,
    coalesce(p.order_paid_inr, 0)                      as paid_inr,
    p.payment_status_final,
    s.delivery_days,
    s.is_late,
    (o.order_status = 'cancelled')::int               as is_cancelled,
    (o.order_status = 'returned')::int                as is_returned,
    (o.order_status = 'delivered')::int               as is_delivered
from orders o
left join items i on o.order_id = i.order_id
left join pay   p on o.order_id = p.order_id
left join ship  s on o.order_id = s.order_id
left join cust  on o.customer_id = cust.customer_id
