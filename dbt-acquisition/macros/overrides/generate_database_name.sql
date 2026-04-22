{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {%- if target.name == 'prod' -%}
{{ 'curated_prod' }}
    {%- elif target.name == 'sit' -%}
{{ 'curated_sit' }}
    {%- elif target.name == 'dev' -%}
{{ 'curated_dev' }}
    {%- else -%}
{{ 'curated_sandbox' }}
    {%- endif -%}
{%- endmacro %}
