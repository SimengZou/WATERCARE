{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld171 Taxonomy Accounts table, Generated 2026-04-10T19:41:43Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['taxo'], type='varchar', alias='taxo') }},
        {{ trx_json_extract('src', ['vers'], type='int', alias='vers') }},
        {{ trx_json_extract('src', ['acid'], type='varchar', alias='acid') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['subl'], type='int', alias='subl') }},
        {{ trx_json_extract('src', ['paci'], type='varchar', alias='paci') }},
        {{ trx_json_extract('src', ['acty'], type='int', alias='acty') }},
        {{ trx_json_extract('src', ['acty_kw'], type='varchar', alias='acty_kw') }},
        {{ trx_json_extract('src', ['mapp'], type='int', alias='mapp') }},
        {{ trx_json_extract('src', ['mapp_kw'], type='varchar', alias='mapp_kw') }},
        {{ trx_json_extract('src', ['unas'], type='int', alias='unas') }},
        {{ trx_json_extract('src', ['unas_kw'], type='varchar', alias='unas_kw') }},
        {{ trx_json_extract('src', ['pseq'], type='int', alias='pseq') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['taxo_vers_ref_compnr'], type='int', alias='taxo_vers_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld171') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'taxo', 'vers', 'acid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
