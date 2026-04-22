{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppdm801 Employee Project Data table, Generated 2026-04-10T19:42:03Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['ctrg'], type='varchar', alias='ctrg') }},
        {{ trx_json_extract('src', ['dcpu'], type='float', alias='dcpu') }},
        {{ trx_json_extract('src', ['usyn'], type='int', alias='usyn') }},
        {{ trx_json_extract('src', ['usyn_kw'], type='varchar', alias='usyn_kw') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['ctrg_ref_compnr'], type='int', alias='ctrg_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppdm801') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'emno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
