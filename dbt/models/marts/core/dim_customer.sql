{# Current-state customer dimension. Grain: one row per customer. #}
select
    customer_id,
    customer_name,
    email,
    phone,
    city,
    state,
    pincode,
    signup_date,
    customer_segment
from {{ ref('stg_customers') }}
