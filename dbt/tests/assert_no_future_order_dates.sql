-- No order should have a purchase date in the future.
select order_id, order_date
from {{ ref('stg_orders') }}
where order_date > current_date
