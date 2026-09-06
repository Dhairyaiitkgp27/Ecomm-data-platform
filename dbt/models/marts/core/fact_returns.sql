{#
  GRAIN: one row per returned order line.
  Measures: return_qty, refund_amount_inr, days_to_return_complete.
#}
select
    r.return_id,
    r.order_id,
    r.order_item_id,
    r.product_id,
    o.customer_id,
    {{ date_key('r.return_requested_ts') }} as return_date_key,
    r.return_reason,
    r.return_status,
    r.return_qty,
    r.refund_amount_inr,
    r.days_to_return_complete
from {{ ref('stg_returns') }} r
join {{ ref('stg_orders') }} o on r.order_id = o.order_id
