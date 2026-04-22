{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PMFORECASTPREVIEW',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PFV_LASTSAVED'], type='timestamp_ntz', alias='pfv_lastsaved') }},
        {{ trx_json_extract('src', ['PFV_SELECT'], type='varchar', alias='pfv_select') }},
        {{ trx_json_extract('src', ['PFV_OBJECT'], type='varchar', alias='pfv_object') }},
        {{ trx_json_extract('src', ['PFV_PARENT'], type='varchar', alias='pfv_parent') }},
        {{ trx_json_extract('src', ['PFV_PARENT_ORG'], type='varchar', alias='pfv_parent_org') }},
        {{ trx_json_extract('src', ['PFV_UPDATECOUNT'], type='float', alias='pfv_updatecount') }},
        {{ trx_json_extract('src', ['PFV_SESSIONID'], type='float', alias='pfv_sessionid') }},
        {{ trx_json_extract('src', ['PFV_OBJECT_ORG'], type='varchar', alias='pfv_object_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pmforecastpreview') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pfv_sessionid', 'pfv_object', 'pfv_object_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pfv_lastsaved']) }}
