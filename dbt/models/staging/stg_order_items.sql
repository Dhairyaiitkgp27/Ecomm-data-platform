with src as (select * from {{ source('staging', 'order_items') }})
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    quantity,
    unit_price_inr,
    coalesce(discount_inr, 0) as discount_inr,
    coalesce(freight_inr, 0)  as freight_inr,
    line_total_inr,
    category,
    subcategory,
    brand
from src
