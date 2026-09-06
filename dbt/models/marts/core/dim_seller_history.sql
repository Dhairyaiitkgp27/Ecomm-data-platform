{#
  SCD Type 2 seller dimension, built from the dbt snapshot.
  Grain: one row per seller *version*. Exposes the standard SCD2 fields:
  effective_start_date, effective_end_date, is_current, plus a surrogate
  version key. Query this when you need a seller attribute as-of a past date.
#}
select
    {{ dbt_utils.generate_surrogate_key(['seller_id', 'dbt_valid_from']) }} as seller_version_key,
    seller_id,
    seller_name,
    seller_city,
    seller_state,
    seller_tier,
    seller_rating,
    business_type,
    dbt_valid_from                          as effective_start_date,
    dbt_valid_to                            as effective_end_date,
    (dbt_valid_to is null)                  as is_current
from {{ ref('sellers_snapshot') }}
