{% macro trx_etl_columns() %}
    {%- set sql_block -%}
cast(etl_load_datetime as datetime) as etl_load_datetime,
cast(etl_load_metadatafilename as varchar) as etl_load_metadatafilename
    {%- endset -%}

    {{- sql_block | trim | indent(width=8) -}}
{% endmacro %}
