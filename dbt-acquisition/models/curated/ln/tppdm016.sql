{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppdm016 Trade Groups table, Generated 2026-04-10T19:41:54Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['ctrg'], type='varchar', alias='ctrg') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['clrt'], type='varchar', alias='clrt') }},
        {{ trx_json_extract('src', ['ltlr'], type='timestamp_ntz', alias='ltlr') }},
        {{ trx_json_extract('src', ['nuem'], type='float', alias='nuem') }},
        {{ trx_json_extract('src', ['mine'], type='float', alias='mine') }},
        {{ trx_json_extract('src', ['mane'], type='float', alias='mane') }},
        {{ trx_json_extract('src', ['dcpu'], type='float', alias='dcpu') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['mtrg'], type='varchar', alias='mtrg') }},
        {{ trx_json_extract('src', ['avtp'], type='varchar', alias='avtp') }},
        {{ trx_json_extract('src', ['clrt_ref_compnr'], type='int', alias='clrt_ref_compnr') }},
        {{ trx_json_extract('src', ['ccal_ref_compnr'], type='int', alias='ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['mtrg_ref_compnr'], type='int', alias='mtrg_ref_compnr') }},
        {{ trx_json_extract('src', ['avtp_ref_compnr'], type='int', alias='avtp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppdm016') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'ctrg']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
