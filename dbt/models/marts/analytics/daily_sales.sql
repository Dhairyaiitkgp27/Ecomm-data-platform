{#
  GRAIN: one row per calendar day. INCREMENTAL (only rebuilds recent days).
  Core daily KPIs for the dashboard trend: GMV, orders, AOV, units, cancels,
  returns and COD share.
#}
{{
  config(
    materialized='incremental',
    unique_key='order_date',
    incremental_strategy='delete+insert',
  )
}}
with o as (
    select * from {{ ref('fact_orders') }}
    {% if is_incremental() %}
    where order_date_key >= (
        select coalesce(max(order_date_key), 0) from {{ this }}
    ) - 3
    {% endif %}
),
by_day as (
    select
        d.full_date                                    as order_date,
        max(d.date_key)                                as order_date_key,
        count(*)                                       as orders,
        count(*) filter (where is_cancelled = 0)       as valid_orders,
        sum(gmv_inr) filter (where is_cancelled = 0)   as gmv_inr,
        sum(total_units) filter (where is_cancelled = 0) as units,
        count(*) filter (where is_cancelled = 1)       as cancelled_orders,
        count(*) filter (where is_returned = 1)        as returned_orders,
        count(*) filter (where is_cod = 1)             as cod_orders
    from o
    join {{ ref('dim_date') }} d on o.order_date_key = d.date_key
    group by d.full_date
)
select
    order_date,
    order_date_key,
    orders,
    valid_orders,
    coalesce(gmv_inr, 0)                            as gmv_inr,
    coalesce(units, 0)                              as units,
    {{ safe_divide('gmv_inr', 'nullif(valid_orders,0)') }} as aov_inr,
    cancelled_orders,
    returned_orders,
    cod_orders,
    {{ safe_divide('cancelled_orders', 'orders') }} as cancellation_rate,
    {{ safe_divide('cod_orders', 'orders') }}       as cod_share
from by_day
