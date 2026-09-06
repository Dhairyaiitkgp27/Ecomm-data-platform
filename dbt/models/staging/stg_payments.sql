with src as (select * from {{ source('staging', 'payments') }})
select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    coalesce(payment_value_inr, 0) as payment_value_inr,
    payment_status,
    coalesce(payment_value_missing, 0) as payment_value_missing
from src
