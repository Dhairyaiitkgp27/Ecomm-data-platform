{#
  GRAIN: one row per order shipment.
  Measures: delivery_days, is_late. Used for delivery/SLA analysis.
#}
select
    s.order_id,
    o.customer_id,
    {{ date_key('o.order_date') }}   as order_date_key,
    s.carrier,
    s.ship_ts,
    s.estimated_delivery_date,
    s.delivered_ts,
    s.delivery_status,
    s.delivery_days,
    s.is_late
from {{ ref('stg_shipments') }} s
join {{ ref('stg_orders') }} o on s.order_id = o.order_id
