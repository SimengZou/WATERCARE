{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WOLABORSCHEDEQUIP',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WLE_LASTSAVED'], type='timestamp_ntz', alias='wle_lastsaved') }},
        {{ trx_json_extract('src', ['WLE_OBJECT'], type='varchar', alias='wle_object') }},
        {{ trx_json_extract('src', ['WLE_SELECT'], type='varchar', alias='wle_select') }},
        {{ trx_json_extract('src', ['WLE_UPDATECOUNT'], type='float', alias='wle_updatecount') }},
        {{ trx_json_extract('src', ['WLE_SESSIONID'], type='float', alias='wle_sessionid') }},
        {{ trx_json_extract('src', ['WLE_OBJECT_ORG'], type='varchar', alias='wle_object_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wolaborschedequip') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wle_sessionid', 'wle_object', 'wle_object_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wle_lastsaved']) }}
