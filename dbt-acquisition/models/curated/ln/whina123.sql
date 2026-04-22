{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whina123 Inventory Revaluation Transaction Cost Details table, Generated 2026-04-10T19:42:46Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['rorg'], type='int', alias='rorg') }},
        {{ trx_json_extract('src', ['rorg_kw'], type='varchar', alias='rorg_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['cpcp'], type='varchar', alias='cpcp') }},
        {{ trx_json_extract('src', ['hour'], type='float', alias='hour') }},
        {{ trx_json_extract('src', ['amnt_1'], type='float', alias='amnt_1') }},
        {{ trx_json_extract('src', ['amnt_2'], type='float', alias='amnt_2') }},
        {{ trx_json_extract('src', ['amnt_3'], type='float', alias='amnt_3') }},
        {{ trx_json_extract('src', ['rorg_orno_seqn_ref_compnr'], type='int', alias='rorg_orno_seqn_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcp_ref_compnr'], type='int', alias='cpcp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whina123') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'rorg', 'orno', 'seqn', 'cpcp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
