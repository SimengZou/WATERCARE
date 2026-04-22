{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WOLABORSCHEDWO',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LSW_LASTSAVED'], type='timestamp_ntz', alias='lsw_lastsaved') }},
        {{ trx_json_extract('src', ['LSW_ACT'], type='float', alias='lsw_act') }},
        {{ trx_json_extract('src', ['LSW_SELECT'], type='varchar', alias='lsw_select') }},
        {{ trx_json_extract('src', ['LSW_HRSREM'], type='float', alias='lsw_hrsrem') }},
        {{ trx_json_extract('src', ['LSW_STATUS'], type='varchar', alias='lsw_status') }},
        {{ trx_json_extract('src', ['LSW_NEWSTATUS'], type='varchar', alias='lsw_newstatus') }},
        {{ trx_json_extract('src', ['LSW_PPLREM'], type='float', alias='lsw_pplrem') }},
        {{ trx_json_extract('src', ['LSW_ACTHRSREM'], type='float', alias='lsw_acthrsrem') }},
        {{ trx_json_extract('src', ['LSW_ACTPPLREQ'], type='float', alias='lsw_actpplreq') }},
        {{ trx_json_extract('src', ['LSW_PREVSCHEDHRS'], type='float', alias='lsw_prevschedhrs') }},
        {{ trx_json_extract('src', ['LSW_PREVSCHEDPPL'], type='float', alias='lsw_prevschedppl') }},
        {{ trx_json_extract('src', ['LSW_TOTALSCHEDHRS'], type='float', alias='lsw_totalschedhrs') }},
        {{ trx_json_extract('src', ['LSW_LASTSCHEDDATE'], type='timestamp_ntz', alias='lsw_lastscheddate') }},
        {{ trx_json_extract('src', ['LSW_MATAVAIL'], type='timestamp_ntz', alias='lsw_matavail') }},
        {{ trx_json_extract('src', ['LSW_RELATED'], type='varchar', alias='lsw_related') }},
        {{ trx_json_extract('src', ['LSW_IGNORE'], type='varchar', alias='lsw_ignore') }},
        {{ trx_json_extract('src', ['LSW_ESIGNUSER'], type='varchar', alias='lsw_esignuser') }},
        {{ trx_json_extract('src', ['LSW_ESIGNTYPE'], type='varchar', alias='lsw_esigntype') }},
        {{ trx_json_extract('src', ['LSW_ESIGNDATE'], type='timestamp_ntz', alias='lsw_esigndate') }},
        {{ trx_json_extract('src', ['LSW_ESIGNCERTIFYNUM'], type='varchar', alias='lsw_esigncertifynum') }},
        {{ trx_json_extract('src', ['LSW_ESIGNCERTIFYTYPE'], type='varchar', alias='lsw_esigncertifytype') }},
        {{ trx_json_extract('src', ['LSW_ACTCOMPLETED'], type='varchar', alias='lsw_actcompleted') }},
        {{ trx_json_extract('src', ['LSW_UPDATECOUNT'], type='float', alias='lsw_updatecount') }},
        {{ trx_json_extract('src', ['LSW_CONTNEXTSHIFT'], type='varchar', alias='lsw_contnextshift') }},
        {{ trx_json_extract('src', ['LSW_DUEPERC'], type='float', alias='lsw_dueperc') }},
        {{ trx_json_extract('src', ['LSW_SESSIONID'], type='float', alias='lsw_sessionid') }},
        {{ trx_json_extract('src', ['LSW_EVENT'], type='varchar', alias='lsw_event') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wolaborschedwo') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['lsw_sessionid', 'lsw_event', 'lsw_act']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lsw_lastsaved']) }}
