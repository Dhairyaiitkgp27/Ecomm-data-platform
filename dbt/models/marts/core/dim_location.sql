{#
  Conformed location dimension from the union of customer and seller geography.
  Grain: one row per (city, state). region and is_metro are derived attributes
  useful for state/region-level rollups in the dashboard.
#}
with locs as (
    select city, state from {{ ref('stg_customers') }}
    union
    select seller_city as city, seller_state as state from {{ ref('stg_sellers') }}
),
distinct_locs as (
    select distinct
        coalesce(nullif(trim(city), ''), 'Unknown')  as city,
        coalesce(nullif(trim(state), ''), 'Unknown') as state
    from locs
)
select
    {{ dbt_utils.generate_surrogate_key(['city', 'state']) }} as location_key,
    city,
    state,
    case
        when state in ('Delhi','Punjab','Haryana','Uttar Pradesh','Uttarakhand',
                       'Rajasthan','Chandigarh','Jammu and Kashmir','Himachal Pradesh') then 'North'
        when state in ('Maharashtra','Gujarat','Goa','Madhya Pradesh','Chhattisgarh') then 'West'
        when state in ('Karnataka','Tamil Nadu','Kerala','Telangana','Andhra Pradesh',
                       'Puducherry') then 'South'
        when state in ('West Bengal','Bihar','Odisha','Jharkhand','Assam','Meghalaya',
                       'Manipur','Mizoram','Sikkim') then 'East'
        else 'Other'
    end as region,
    (city in ('Mumbai','Delhi','Bengaluru','Hyderabad','Chennai','Kolkata',
              'Pune','Ahmedabad','Chandigarh')) as is_metro
from distinct_locs
