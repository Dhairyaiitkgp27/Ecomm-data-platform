{#
  Date dimension generated with generate_series (no seed needed). Covers a range
  wide enough for all order/ship/return dates. Grain: one row per calendar day.
  Includes an Indian fiscal year (Apr-Mar) which is common in Indian reporting.
#}
with spine as (
    select generate_series(
        date '2022-01-01', date '2025-12-31', interval '1 day'
    )::date as d
)
select
    cast(to_char(d, 'YYYYMMDD') as integer)      as date_key,
    d                                            as full_date,
    extract(year  from d)::int                   as year,
    extract(quarter from d)::int                 as quarter,
    extract(month from d)::int                   as month,
    to_char(d, 'Month')                          as month_name,
    extract(day from d)::int                     as day_of_month,
    extract(isodow from d)::int                  as day_of_week,
    to_char(d, 'Day')                            as day_name,
    extract(week from d)::int                    as week_of_year,
    (extract(isodow from d) in (6, 7))           as is_weekend,
    (d = date_trunc('month', d))                 as is_month_start,
    (d = (date_trunc('month', d) + interval '1 month - 1 day'))  as is_month_end,
    case when extract(month from d) >= 4
         then extract(year from d)::int
         else extract(year from d)::int - 1 end  as fiscal_year
from spine
