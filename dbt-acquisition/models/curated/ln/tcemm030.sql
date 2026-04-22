{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcemm030 Enterprise Units table, Generated 2026-04-10T19:41:05Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['eunt'], type='varchar', alias='eunt') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['lcmp'], type='int', alias='lcmp') }},
        {{ trx_json_extract('src', ['fcmp'], type='int', alias='fcmp') }},
        {{ trx_json_extract('src', ['euca'], type='varchar', alias='euca') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['dfpo'], type='varchar', alias='dfpo') }},
        {{ trx_json_extract('src', ['dfco'], type='varchar', alias='dfco') }},
        {{ trx_json_extract('src', ['ract'], type='varchar', alias='ract') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['duns'], type='int', alias='duns') }},
        {{ trx_json_extract('src', ['txtn'], type='int', alias='txtn') }},
        {{ trx_json_extract('src', ['lcmp_ref_compnr'], type='int', alias='lcmp_ref_compnr') }},
        {{ trx_json_extract('src', ['fcmp_ref_compnr'], type='int', alias='fcmp_ref_compnr') }},
        {{ trx_json_extract('src', ['euca_ref_compnr'], type='int', alias='euca_ref_compnr') }},
        {{ trx_json_extract('src', ['ccal_ref_compnr'], type='int', alias='ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['ract_ref_compnr'], type='int', alias='ract_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['txtn_ref_compnr'], type='int', alias='txtn_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcemm030') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'eunt']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
