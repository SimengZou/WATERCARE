{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ACTSCHEDULES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ACS_LASTSAVED'], type='timestamp_ntz', alias='acs_lastsaved') }},
        {{ trx_json_extract('src', ['ACS_COMMENT'], type='varchar', alias='acs_comment') }},
        {{ trx_json_extract('src', ['ACS_MRC'], type='varchar', alias='acs_mrc') }},
        {{ trx_json_extract('src', ['ACS_TRADE'], type='varchar', alias='acs_trade') }},
        {{ trx_json_extract('src', ['ACS_OBJECT'], type='varchar', alias='acs_object') }},
        {{ trx_json_extract('src', ['ACS_SCHED'], type='timestamp_ntz', alias='acs_sched') }},
        {{ trx_json_extract('src', ['ACS_STARTTIME'], type='float', alias='acs_starttime') }},
        {{ trx_json_extract('src', ['ACS_ENDTIME'], type='float', alias='acs_endtime') }},
        {{ trx_json_extract('src', ['ACS_HOURS'], type='float', alias='acs_hours') }},
        {{ trx_json_extract('src', ['ACS_RESPONSIBLE'], type='varchar', alias='acs_responsible') }},
        {{ trx_json_extract('src', ['ACS_SHIFT'], type='varchar', alias='acs_shift') }},
        {{ trx_json_extract('src', ['ACS_SCHEDULER'], type='varchar', alias='acs_scheduler') }},
        {{ trx_json_extract('src', ['ACS_FROZEN'], type='varchar', alias='acs_frozen') }},
        {{ trx_json_extract('src', ['ACS_OBJECT_ORG'], type='varchar', alias='acs_object_org') }},
        {{ trx_json_extract('src', ['ACS_MPPROJ'], type='varchar', alias='acs_mpproj') }},
        {{ trx_json_extract('src', ['ACS_UPDATECOUNT'], type='float', alias='acs_updatecount') }},
        {{ trx_json_extract('src', ['ACS_CODE'], type='varchar', alias='acs_code') }},
        {{ trx_json_extract('src', ['ACS_UPDATED'], type='timestamp_ntz', alias='acs_updated') }},
        {{ trx_json_extract('src', ['ACS_SHIFTSCHEDULESESSION'], type='float', alias='acs_shiftschedulesession') }},
        {{ trx_json_extract('src', ['ACS_EVENT'], type='varchar', alias='acs_event') }},
        {{ trx_json_extract('src', ['ACS_ACTIVITY'], type='float', alias='acs_activity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5actschedules') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['acs_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['acs_lastsaved']) }}
