{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppdm002 Map Session Options by User table, Generated 2026-04-10T19:41:52Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['loco'], type='varchar', alias='loco') }},
        {{ trx_json_extract('src', ['maps'], type='int', alias='maps') }},
        {{ trx_json_extract('src', ['maps_kw'], type='varchar', alias='maps_kw') }},
        {{ trx_json_extract('src', ['mpss'], type='int', alias='mpss') }},
        {{ trx_json_extract('src', ['mpss_kw'], type='varchar', alias='mpss_kw') }},
        {{ trx_json_extract('src', ['mpvw'], type='int', alias='mpvw') }},
        {{ trx_json_extract('src', ['mpvw_kw'], type='varchar', alias='mpvw_kw') }},
        {{ trx_json_extract('src', ['sses'], type='int', alias='sses') }},
        {{ trx_json_extract('src', ['sses_kw'], type='varchar', alias='sses_kw') }},
        {{ trx_json_extract('src', ['ssvw'], type='int', alias='ssvw') }},
        {{ trx_json_extract('src', ['ssvw_kw'], type='varchar', alias='ssvw_kw') }},
        {{ trx_json_extract('src', ['tprf'], type='int', alias='tprf') }},
        {{ trx_json_extract('src', ['tprf_kw'], type='varchar', alias='tprf_kw') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppdm002') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'loco', 'maps']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
