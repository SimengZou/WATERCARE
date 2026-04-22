{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MANUFACTURERS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MFG_LASTSAVED'], type='timestamp_ntz', alias='mfg_lastsaved') }},
        {{ trx_json_extract('src', ['MFG_DESC'], type='varchar', alias='mfg_desc') }},
        {{ trx_json_extract('src', ['MFG_SUPPLIER'], type='varchar', alias='mfg_supplier') }},
        {{ trx_json_extract('src', ['MFG_CLASS'], type='varchar', alias='mfg_class') }},
        {{ trx_json_extract('src', ['MFG_SOURCESYSTEM'], type='varchar', alias='mfg_sourcesystem') }},
        {{ trx_json_extract('src', ['MFG_SOURCECODE'], type='varchar', alias='mfg_sourcecode') }},
        {{ trx_json_extract('src', ['MFG_CLASS_ORG'], type='varchar', alias='mfg_class_org') }},
        {{ trx_json_extract('src', ['MFG_SUPPLIER_ORG'], type='varchar', alias='mfg_supplier_org') }},
        {{ trx_json_extract('src', ['MFG_NOTUSED'], type='varchar', alias='mfg_notused') }},
        {{ trx_json_extract('src', ['MFG_UPDATECOUNT'], type='float', alias='mfg_updatecount') }},
        {{ trx_json_extract('src', ['MFG_UPDATED'], type='timestamp_ntz', alias='mfg_updated') }},
        {{ trx_json_extract('src', ['MFG_CODE'], type='varchar', alias='mfg_code') }},
        {{ trx_json_extract('src', ['MFG_ORG'], type='varchar', alias='mfg_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5manufacturers') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mfg_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mfg_lastsaved']) }}
