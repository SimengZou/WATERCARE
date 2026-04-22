{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTWO',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ALW_LASTSAVED'], type='timestamp_ntz', alias='alw_lastsaved') }},
        {{ trx_json_extract('src', ['ALW_DELAYUOM'], type='varchar', alias='alw_delayuom') }},
        {{ trx_json_extract('src', ['ALW_STANDWORK'], type='varchar', alias='alw_standwork') }},
        {{ trx_json_extract('src', ['ALW_WORKORDERORG'], type='varchar', alias='alw_workorderorg') }},
        {{ trx_json_extract('src', ['ALW_OBJECTFIELDID'], type='float', alias='alw_objectfieldid') }},
        {{ trx_json_extract('src', ['ALW_OBJECTORGFIELDID'], type='float', alias='alw_objectorgfieldid') }},
        {{ trx_json_extract('src', ['ALW_WORKORDERORGFIELDID'], type='float', alias='alw_workorderorgfieldid') }},
        {{ trx_json_extract('src', ['ALW_PROBLEMCODEFIELDID'], type='float', alias='alw_problemcodefieldid') }},
        {{ trx_json_extract('src', ['ALW_JOBTYPEFIELDID'], type='float', alias='alw_jobtypefieldid') }},
        {{ trx_json_extract('src', ['ALW_PRIORITYFIELDID'], type='float', alias='alw_priorityfieldid') }},
        {{ trx_json_extract('src', ['ALW_DURATIONFIELDID'], type='float', alias='alw_durationfieldid') }},
        {{ trx_json_extract('src', ['ALW_SCHEDSTARTFIELDID'], type='float', alias='alw_schedstartfieldid') }},
        {{ trx_json_extract('src', ['ALW_REQUESTSTARTFIELDID'], type='float', alias='alw_requeststartfieldid') }},
        {{ trx_json_extract('src', ['ALW_REQUESTENDFIELDID'], type='float', alias='alw_requestendfieldid') }},
        {{ trx_json_extract('src', ['ALW_WODESC'], type='varchar', alias='alw_wodesc') }},
        {{ trx_json_extract('src', ['ALW_WOCOMMENT'], type='varchar', alias='alw_wocomment') }},
        {{ trx_json_extract('src', ['ALW_UPDATECOUNT'], type='float', alias='alw_updatecount') }},
        {{ trx_json_extract('src', ['ALW_INCLUDENONCONFORMITIES'], type='varchar', alias='alw_includenonconformities') }},
        {{ trx_json_extract('src', ['ALW_DUENONCONFORMITIESONLY'], type='varchar', alias='alw_duenonconformitiesonly') }},
        {{ trx_json_extract('src', ['ALW_ALERT'], type='varchar', alias='alw_alert') }},
        {{ trx_json_extract('src', ['ALW_DELAY'], type='float', alias='alw_delay') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alertwo') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['alw_alert']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['alw_lastsaved']) }}
