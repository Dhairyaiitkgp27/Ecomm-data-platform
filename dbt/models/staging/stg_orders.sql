with src as (select * from {{ source('staging', 'orders') }})
select
    order_id,
    customer_id,
    lower(order_status)                 as order_status,
    cast(order_purchase_ts as timestamp) as order_purchase_ts,
    cast(order_approved_ts as timestamp) as order_approved_ts,
    cast(order_date as date)            as order_date,
    payment_type,
    is_cod,
    customer_order_seq,
    is_repeat_order
from src
