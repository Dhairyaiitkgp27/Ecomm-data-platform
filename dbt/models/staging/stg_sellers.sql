with src as (select * from {{ source('staging', 'sellers') }})
select
    seller_id,
    seller_name,
    seller_city,
    seller_state,
    seller_tier,
    seller_pincode,
    cast(onboarded_date as date) as onboarded_date,
    seller_rating,
    business_type
from src
