{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN ttaad420 Logical/Physical Company Number table, Generated 2026-04-10T19:42:39Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['tabg'], type='int', alias='tabg') }},
        {{ trx_json_extract('src', ['tabg_kw'], type='varchar', alias='tabg_kw') }},
        {{ trx_json_extract('src', ['tabl'], type='varchar', alias='tabl') }},
        {{ trx_json_extract('src', ['lcmp'], type='int', alias='lcmp') }},
        {{ trx_json_extract('src', ['fcmp'], type='int', alias='fcmp') }},
        {{ trx_json_extract('src', ['lcmp_ref_compnr'], type='int', alias='lcmp_ref_compnr') }},
        {{ trx_json_extract('src', ['fcmp_ref_compnr'], type='int', alias='fcmp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_ttaad420') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'tabg', 'tabl', 'lcmp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
