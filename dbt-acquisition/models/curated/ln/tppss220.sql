{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppss220 Activity Baselines table, Generated 2026-04-10T19:42:19Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['basn'], type='int', alias='basn') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['pact'], type='varchar', alias='pact') }},
        {{ trx_json_extract('src', ['tact'], type='int', alias='tact') }},
        {{ trx_json_extract('src', ['tact_kw'], type='varchar', alias='tact_kw') }},
        {{ trx_json_extract('src', ['bsdt'], type='timestamp_ntz', alias='bsdt') }},
        {{ trx_json_extract('src', ['bfdt'], type='timestamp_ntz', alias='bfdt') }},
        {{ trx_json_extract('src', ['cprj_basn_ref_compnr'], type='int', alias='cprj_basn_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['actv_cpla'], type='varchar', alias='actv_cpla') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppss220') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'basn', 'cact']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
