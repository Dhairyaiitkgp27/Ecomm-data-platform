{#
  Current-state seller dimension used for most fact joins.
  Grain: one row per seller (unique). For point-in-time attribute values use
  dim_seller_history (SCD2).
#}
select
    seller_id,
    seller_name,
    seller_city,
    seller_state,
    seller_tier,
    seller_pincode,
    onboarded_date,
    seller_rating,
    business_type
from {{ ref('stg_sellers') }}
