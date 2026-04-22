
{% macro trx_audit_columns() %}
    {%- set sql_block -%}
current_timestamp() as dbt_record_updated_at,
current_timestamp() as dbt_record_created_at,
cast('{{ invocation_id }}' as varchar) as dbt_invocation_id
    {%- endset -%}

    {{- sql_block | trim | indent(width=4) -}}
{% endmacro %}