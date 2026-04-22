{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinr130 Item Issue by Warehouse and Period table, Generated 2025-06-12T01:48:43Z from package combination ce01090',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['peri'], type='int', alias='peri') }},
        {{ trx_json_extract('src', ['acip'], type='float', alias='acip') }},
        {{ trx_json_extract('src', ['decl'], type='float', alias='decl') }},
        {{ trx_json_extract('src', ['deqt'], type='float', alias='deqt') }},
        {{ trx_json_extract('src', ['qdeq'], type='float', alias='qdeq') }},
        {{ trx_json_extract('src', ['lsts'], type='float', alias='lsts') }},
        {{ trx_json_extract('src', ['dmfp'], type='float', alias='dmfp') }},
        {{ trx_json_extract('src', ['rets'], type='float', alias='rets') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinr130') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cwar', 'item', 'year', 'peri']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
