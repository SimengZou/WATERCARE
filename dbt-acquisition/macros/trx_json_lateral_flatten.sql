{% macro trx_lateral_flatten(column, path, alias=none, strict=true) %}
    {%- set path_array = path.split('.') if path is string else path -%}
    {%- set final_alias = alias if alias else path_array | join('_') -%}

    {%- set sql_block -%}
lateral flatten(input => {{ column }}:{{ path_array | safe }}) as {{ final_alias }}
    {%- endset -%}

    {{- sql_block | trim | indent(width=4) -}}
{% endmacro %}
