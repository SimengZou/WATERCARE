{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USAGE',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SUS_LASTSAVED'], type='timestamp_ntz', alias='sus_lastsaved') }},
        {{ trx_json_extract('src', ['SUS_TARGET_TAB'], type='varchar', alias='sus_target_tab') }},
        {{ trx_json_extract('src', ['SUS_SERVERID'], type='varchar', alias='sus_serverid') }},
        {{ trx_json_extract('src', ['SUS_SESSIONID'], type='float', alias='sus_sessionid') }},
        {{ trx_json_extract('src', ['SUS_USERAGENT'], type='varchar', alias='sus_useragent') }},
        {{ trx_json_extract('src', ['SUS_PROCESSING_TIME'], type='float', alias='sus_processing_time') }},
        {{ trx_json_extract('src', ['SUS_ACTION_DATE'], type='timestamp_ntz', alias='sus_action_date') }},
        {{ trx_json_extract('src', ['SUS_UPDATECOUNT'], type='float', alias='sus_updatecount') }},
        {{ trx_json_extract('src', ['SUS_ID'], type='varchar', alias='sus_id') }},
        {{ trx_json_extract('src', ['SUS_ACTION'], type='varchar', alias='sus_action') }},
        {{ trx_json_extract('src', ['SUS_TYPE'], type='varchar', alias='sus_type') }},
        {{ trx_json_extract('src', ['SUS_SUBTYPE'], type='varchar', alias='sus_subtype') }},
        {{ trx_json_extract('src', ['SUS_TARGET'], type='varchar', alias='sus_target') }},
        {{ trx_json_extract('src', ['SUS_TARGET_PARENT'], type='varchar', alias='sus_target_parent') }},
        {{ trx_json_extract('src', ['SUS_USERID'], type='varchar', alias='sus_userid') }},
        {{ trx_json_extract('src', ['SUS_TENANT'], type='varchar', alias='sus_tenant') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5usage') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['sus_id']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sus_lastsaved']) }}
