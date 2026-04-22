{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tccom155 Business Partner - Satellites table, Generated 2026-04-10T19:41:04Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['cofc'], type='varchar', alias='cofc') }},
        {{ trx_json_extract('src', ['kcod'], type='int', alias='kcod') }},
        {{ trx_json_extract('src', ['kcod_kw'], type='varchar', alias='kcod_kw') }},
        {{ trx_json_extract('src', ['code'], type='varchar', alias='code') }},
        {{ trx_json_extract('src', ['cert', 'en_US'], type='varchar', alias='cert__en_us') }},
        {{ trx_json_extract('src', ['efdt'], type='timestamp_ntz', alias='efdt') }},
        {{ trx_json_extract('src', ['exdt'], type='timestamp_ntz', alias='exdt') }},
        {{ trx_json_extract('src', ['bpid_ref_compnr'], type='int', alias='bpid_ref_compnr') }},
        {{ trx_json_extract('src', ['cofc_ref_compnr'], type='int', alias='cofc_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tccom155') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'bpid', 'cofc', 'kcod', 'code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
