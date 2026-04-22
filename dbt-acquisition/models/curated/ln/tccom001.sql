{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tccom001 Employees - General table, Generated 2026-04-10T19:41:00Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['nama', 'en_US'], type='varchar', alias='nama__en_us') }},
        {{ trx_json_extract('src', ['namb', 'en_US'], type='varchar', alias='namb__en_us') }},
        {{ trx_json_extract('src', ['namc', 'en_US'], type='varchar', alias='namc__en_us') }},
        {{ trx_json_extract('src', ['namd', 'en_US'], type='varchar', alias='namd__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['cwoc'], type='varchar', alias='cwoc') }},
        {{ trx_json_extract('src', ['clrt'], type='varchar', alias='clrt') }},
        {{ trx_json_extract('src', ['cpcp'], type='varchar', alias='cpcp') }},
        {{ trx_json_extract('src', ['clan'], type='varchar', alias='clan') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['loco'], type='varchar', alias='loco') }},
        {{ trx_json_extract('src', ['imag'], type='varchar', alias='imag') }},
        {{ trx_json_extract('src', ['guid'], type='varchar', alias='guid') }},
        {{ trx_json_extract('src', ['ctit'], type='varchar', alias='ctit') }},
        {{ trx_json_extract('src', ['cdf_emno'], type='varchar', alias='cdf_emno') }},
        {{ trx_json_extract('src', ['cwoc_ref_compnr'], type='int', alias='cwoc_ref_compnr') }},
        {{ trx_json_extract('src', ['clrt_ref_compnr'], type='int', alias='clrt_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcp_ref_compnr'], type='int', alias='cpcp_ref_compnr') }},
        {{ trx_json_extract('src', ['clan_ref_compnr'], type='int', alias='clan_ref_compnr') }},
        {{ trx_json_extract('src', ['ccal_ref_compnr'], type='int', alias='ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['ctit_ref_compnr'], type='int', alias='ctit_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tccom001') }}
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
