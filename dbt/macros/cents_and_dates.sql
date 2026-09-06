{# Small reusable macros used across marts. #}

{# Build an integer date key YYYYMMDD from a date/timestamp column. #}
{% macro date_key(col) %}
    cast(to_char(cast({{ col }} as date), 'YYYYMMDD') as integer)
{% endmacro %}

{# Safe division that returns 0 instead of erroring / null on divide-by-zero. #}
{% macro safe_divide(numerator, denominator) %}
    case when coalesce({{ denominator }}, 0) = 0 then 0
         else {{ numerator }}::numeric / {{ denominator }} end
{% endmacro %}
