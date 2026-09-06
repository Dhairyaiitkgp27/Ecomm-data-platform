{#
  SCD Type 2 history for sellers.

  dbt's snapshot mechanism tracks changes to the chosen columns over time. On
  each `dbt snapshot` run it compares the current source row to the latest
  snapshot row; when a tracked attribute changes it closes the old version
  (sets dbt_valid_to) and inserts a new current version. This is a genuine,
  production-standard SCD2 implementation with no hand-written merge logic.

  To SEE history appear in a demo: run `dbt snapshot`, then apply
  sql/scd2_demo_update.sql (changes a seller's rating/city), then run
  `dbt snapshot` again -> that seller now has two versions.
#}
{% snapshot sellers_snapshot %}
    {{
        config(
          target_schema='snapshots',
          unique_key='seller_id',
          strategy='check',
          check_cols=['seller_rating', 'business_type', 'seller_city', 'seller_state'],
          invalidate_hard_deletes=True,
        )
    }}
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
    from {{ source('staging', 'sellers') }}
{% endsnapshot %}
