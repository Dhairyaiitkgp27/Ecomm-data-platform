{#
  Use the schema configured on the model as-is (e.g. 'marts', 'analytics')
  instead of dbt's default of prefixing it with the target schema. Keeps the
  warehouse layout predictable: marts.dim_customer, analytics.daily_sales, etc.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
