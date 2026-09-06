{#
  Collapse payments to order grain: total value, max installments, and a single
  representative payment status (the "strongest" outcome for the order).
#}
with payments as (
    select * from {{ ref('stg_payments') }}
)
select
    order_id,
    sum(payment_value_inr)                                   as order_paid_inr,
    max(payment_installments)                                as max_installments,
    count(*)                                                 as n_payment_records,
    -- collapse status: refunded > paid > pending > failed > unknown
    case
        when bool_or(payment_status = 'refunded') then 'refunded'
        when bool_or(payment_status = 'paid')     then 'paid'
        when bool_or(payment_status = 'pending')  then 'pending'
        when bool_or(payment_status = 'failed')   then 'failed'
        else 'unknown'
    end                                                      as payment_status_final
from payments
group by order_id
