{% macro trx_audit_surrogate(columns=none) %}
    {%- set sql_block -%}
    {%- if columns -%}
md5_binary(concat_ws('||',
{%- for col in columns %}
    coalesce(cast({{ col }} as string), '_null_'){% if not loop.last %},{% endif %}
{%- endfor %}
)) as dbt_surrogate_key
    {%- else -%}
md5_binary(cast(object_construct(* exclude (
    etl_load_datetime,
    etl_load_metadatafilename
)) as string)) as dbt_surrogate_key
    {%- endif -%}
    {%- endset -%}

    {{- sql_block | trim | indent(width=4) -}}
{% endmacro %}
