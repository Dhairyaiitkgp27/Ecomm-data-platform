with src as (select * from {{ source('staging', 'shipments') }})
select
    order_id,
    carrier,
    cast(ship_ts as timestamp)              as ship_ts,
    cast(estimated_delivery_date as date)   as estimated_delivery_date,
    cast(delivered_ts as timestamp)         as delivered_ts,
    delivery_status,
    delivery_days,
    is_late
from src
