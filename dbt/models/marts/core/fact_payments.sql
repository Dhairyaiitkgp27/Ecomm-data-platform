{#
  GRAIN: one row per payment record (order_id + payment_sequential).
  FK: order_id -> fact_orders / dim (degenerate). Measures: payment_value_inr,
  payment_installments.
#}
select
    p.order_id,
    p.payment_sequential,
    o.customer_id,
    {{ date_key('o.order_date') }}   as order_date_key,
    p.payment_type,
    p.payment_status,
    p.payment_installments,
    p.payment_value_inr,
    p.payment_value_missing
from {{ ref('stg_payments') }} p
join {{ ref('stg_orders') }} o on p.order_id = o.order_id
