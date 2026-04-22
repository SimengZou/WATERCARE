{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PMFORECASTEQUIPLIST',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PFL_LASTSAVED'], type='timestamp_ntz', alias='pfl_lastsaved') }},
        {{ trx_json_extract('src', ['PFL_OBJECT'], type='varchar', alias='pfl_object') }},
        {{ trx_json_extract('src', ['PFL_OBJECT_ORG'], type='varchar', alias='pfl_object_org') }},
        {{ trx_json_extract('src', ['PFL_SELECT'], type='varchar', alias='pfl_select') }},
        {{ trx_json_extract('src', ['PFL_PARENT_ORG'], type='varchar', alias='pfl_parent_org') }},
        {{ trx_json_extract('src', ['PFL_UPDATECOUNT'], type='float', alias='pfl_updatecount') }},
        {{ trx_json_extract('src', ['PFL_SESSIONID'], type='float', alias='pfl_sessionid') }},
        {{ trx_json_extract('src', ['PFL_PARENT'], type='varchar', alias='pfl_parent') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pmforecastequiplist') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pfl_sessionid', 'pfl_object', 'pfl_object_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pfl_lastsaved']) }}
