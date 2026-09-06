with src as (select * from {{ source('staging', 'products') }})
select
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    mrp_inr,
    weight_grams
from src
