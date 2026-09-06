with src as (select * from {{ source('staging', 'customers') }})
select
    customer_id,
    customer_name,
    email,
    phone,
    coalesce(city, 'Unknown')  as city,
    coalesce(state, 'Unknown') as state,
    pincode,
    cast(signup_date as date)  as signup_date,
    customer_segment
from src
