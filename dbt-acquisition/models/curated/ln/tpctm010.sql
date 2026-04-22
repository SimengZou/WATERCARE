{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpctm010 Programs table, Generated 2026-04-10T19:41:50Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['prgm'], type='varchar', alias='prgm') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['mngr'], type='varchar', alias='mngr') }},
        {{ trx_json_extract('src', ['spns'], type='varchar', alias='spns') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['mngr_ref_compnr'], type='int', alias='mngr_ref_compnr') }},
        {{ trx_json_extract('src', ['spns_ref_compnr'], type='int', alias='spns_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpctm010') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'prgm']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
