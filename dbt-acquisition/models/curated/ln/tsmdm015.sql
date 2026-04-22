{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsmdm015 Tasks table, Generated 2026-04-10T19:42:36Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['ctsk'], type='varchar', alias='ctsk') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['sear', 'en_US'], type='varchar', alias='sear__en_us') }},
        {{ trx_json_extract('src', ['clrt'], type='varchar', alias='clrt') }},
        {{ trx_json_extract('src', ['occf'], type='float', alias='occf') }},
        {{ trx_json_extract('src', ['cpcp'], type='varchar', alias='cpcp') }},
        {{ trx_json_extract('src', ['tmdu'], type='float', alias='tmdu') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['clrt_ref_compnr'], type='int', alias='clrt_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcp_ref_compnr'], type='int', alias='cpcp_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsmdm015') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'ctsk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
