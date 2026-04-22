{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs005 Reasons table, Generated 2026-04-10T19:41:09Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cdis'], type='varchar', alias='cdis') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['rstp'], type='int', alias='rstp') }},
        {{ trx_json_extract('src', ['rstp_kw'], type='varchar', alias='rstp_kw') }},
        {{ trx_json_extract('src', ['drpi'], type='int', alias='drpi') }},
        {{ trx_json_extract('src', ['drpi_kw'], type='varchar', alias='drpi_kw') }},
        {{ trx_json_extract('src', ['appr'], type='int', alias='appr') }},
        {{ trx_json_extract('src', ['appr_kw'], type='varchar', alias='appr_kw') }},
        {{ trx_json_extract('src', ['bilb'], type='int', alias='bilb') }},
        {{ trx_json_extract('src', ['bilb_kw'], type='varchar', alias='bilb_kw') }},
        {{ trx_json_extract('src', ['efdt'], type='timestamp_ntz', alias='efdt') }},
        {{ trx_json_extract('src', ['exdt'], type='timestamp_ntz', alias='exdt') }},
        {{ trx_json_extract('src', ['etrt'], type='int', alias='etrt') }},
        {{ trx_json_extract('src', ['etrt_kw'], type='varchar', alias='etrt_kw') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs005') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cdis']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
