{% macro trx_json_extract(column, path, type='varchar', alias=none, strict=true) %}
    {%- set path_array = path.split('.') if path is string else path -%}
    {%- set ns = namespace(formatted_cols=[]) -%}
    
    {%- for p in path_array -%}
        {%- if '.' in p -%}
            {%- do ns.formatted_cols.append('"' ~ p ~ '"') -%}
        {%- else -%}
            {%- do ns.formatted_cols.append(p) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set final_alias = alias if alias else path_array | map('replace', '.', '_') | join('_') -%}
    {%- set path_string = ns.formatted_cols | join('.') -%}
    {%- set cast_func = 'cast' if strict else 'try_cast' -%}

    {%- set sql_block -%}
case 
    when is_null_value(get_path({{ column }}, '{{ path_string }}')) then null
    else {{ cast_func }}(get_path({{ column }}, '{{ path_string }}') as {{ type }})
end as {{ final_alias }}
    {%- endset -%}

    {{- sql_block | trim | indent(width=8) -}}
{% endmacro %}
