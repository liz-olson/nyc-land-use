{#
  By default dbt names schemas "<target schema>_<custom schema>" (e.g. main_silver).
  This override uses the custom schema as-is, so tables land in clean schemas:
  silver.stg_zap__projects, gold.fct_land_use_projects.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
