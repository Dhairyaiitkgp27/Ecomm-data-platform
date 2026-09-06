# Data dictionary — star schema

Schemas in PostgreSQL: `marts.*` (dimensions + facts) and `analytics.*`
(business marts). Grain is stated for every fact.

## Dimensions

### dim_date  — grain: one calendar day
`date_key` (PK, int YYYYMMDD), `full_date`, `year`, `quarter`, `month`,
`month_name`, `day_of_month`, `day_of_week`, `week_of_year`, `is_weekend`,
`is_month_start`, `is_month_end`, `fiscal_year` (Apr–Mar).

### dim_customer — grain: one customer
`customer_id` (PK), `customer_name`, `email`, `phone`, `city`, `state`,
`pincode`, `signup_date`, `customer_segment`.

### dim_product — grain: one product
`product_id` (PK), `product_name`, `category`, `subcategory`, `brand`,
`mrp_inr`, `weight_grams`, `weight_band`.

### dim_seller — grain: one seller (current state)
`seller_id` (PK), `seller_name`, `seller_city`, `seller_state`, `seller_tier`,
`seller_pincode`, `onboarded_date`, `seller_rating`, `business_type`.

### dim_seller_history — grain: one seller **version** (SCD Type 2)
`seller_version_key` (PK, surrogate), `seller_id`, attributes…,
`effective_start_date`, `effective_end_date`, `is_current`.
Built from a dbt snapshot; query for point-in-time seller attributes.

### dim_location — grain: one (city, state)
`location_key` (PK, surrogate), `city`, `state`, `region`, `is_metro`.

## Facts

### fact_orders — grain: **one row per order**
- Keys: `order_id` (PK), `customer_id` (FK→dim_customer),
  `order_date_key` (FK→dim_date), `location_key` (FK→dim_location).
- Degenerate: `order_status`, `payment_type`.
- Measures: `gmv_inr`, `order_value_inr`, `total_units`, `n_items`,
  `discount_inr`, `freight_inr`, `paid_inr`, `delivery_days`.
- Flags: `is_cod`, `is_repeat_order`, `is_cancelled`, `is_returned`,
  `is_delivered`, `is_late`.

### fact_order_items — grain: **one row per product line within an order**
- Keys: (`order_id`, `order_item_id`) (PK), `product_id` (FK→dim_product),
  `seller_id` (FK→dim_seller), `customer_id`, `order_date_key` (FK→dim_date).
- Measures: `quantity`, `unit_price_inr`, `discount_inr`, `freight_inr`,
  `line_total_inr`, `gross_line_value_inr`.
- **Incremental** dbt model (delete+insert on composite key).

### fact_payments — grain: **one row per payment record**
- Keys: (`order_id`, `payment_sequential`) (PK).
- Measures: `payment_value_inr`, `payment_installments`; degenerate
  `payment_type`, `payment_status`.

### fact_shipments — grain: **one row per order shipment**
- Keys: `order_id`, `order_date_key` (FK→dim_date).
- Measures: `delivery_days`, `is_late`; attributes `carrier`, `ship_ts`,
  `estimated_delivery_date`, `delivered_ts`, `delivery_status`.

### fact_returns — grain: **one row per returned order line**
- Keys: `return_id` (PK), `order_id`, `order_item_id`, `product_id`.
- Measures: `return_qty`, `refund_amount_inr`, `days_to_return_complete`;
  attributes `return_reason`, `return_status`.

## Relationships (star)

```
        dim_date ─────┐        ┌───── dim_customer
                      │        │
        dim_location ─┼─ fact_orders ─┼─ dim_product (via items)
                      │        │
        dim_seller ───┘   fact_order_items ── dim_product / dim_seller / dim_date
                          fact_payments / fact_shipments / fact_returns
```
