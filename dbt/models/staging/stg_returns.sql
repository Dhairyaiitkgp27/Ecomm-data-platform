with src as (select * from {{ source('staging', 'returns') }})
select
    return_id,
    order_id,
    order_item_id,
    product_id,
    return_reason,
    return_status,
    return_qty,
    refund_amount_inr,
    cast(return_requested_ts as timestamp) as return_requested_ts,
    cast(return_completed_ts as timestamp) as return_completed_ts,
    days_to_return_complete
from src
