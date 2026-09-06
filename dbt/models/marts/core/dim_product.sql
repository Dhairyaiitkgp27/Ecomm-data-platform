{# Product dimension. Grain: one row per product. #}
select
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    mrp_inr,
    weight_grams,
    case
        when weight_grams < 500   then 'Light'
        when weight_grams < 5000  then 'Medium'
        else 'Heavy'
    end as weight_band
from {{ ref('stg_products') }}
