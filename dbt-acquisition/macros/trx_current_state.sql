{% macro trx_current_state(order_by=['etl_load_datetime']) %}
    {%- set sql_block -%}
qualify row_number() over (
    partition by dbt_surrogate_key 
    order by {{ order_by | join(' desc, ') }} desc
) = 1
    {%- endset -%}

    {{- sql_block | trim | indent(width=0) -}}
{% endmacro %}
