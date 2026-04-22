{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PMPLANSESSION',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PPS_LASTSAVED'], type='timestamp_ntz', alias='pps_lastsaved') }},
        {{ trx_json_extract('src', ['PPS_SESSIONID'], type='float', alias='pps_sessionid') }},
        {{ trx_json_extract('src', ['PPS_PPOPK'], type='float', alias='pps_ppopk') }},
        {{ trx_json_extract('src', ['PPS_PMREVISION'], type='float', alias='pps_pmrevision') }},
        {{ trx_json_extract('src', ['PPS_DUEWEEKOFMONTH'], type='varchar', alias='pps_dueweekofmonth') }},
        {{ trx_json_extract('src', ['PPS_DUEDAYOFWEEK'], type='float', alias='pps_duedayofweek') }},
        {{ trx_json_extract('src', ['PPS_DUEDATE'], type='timestamp_ntz', alias='pps_duedate') }},
        {{ trx_json_extract('src', ['PPS_NESTINGREF'], type='varchar', alias='pps_nestingref') }},
        {{ trx_json_extract('src', ['PPS_IGNOREFREQWARNING'], type='varchar', alias='pps_ignorefreqwarning') }},
        {{ trx_json_extract('src', ['PPS_IGNORERANGEWARNING'], type='varchar', alias='pps_ignorerangewarning') }},
        {{ trx_json_extract('src', ['PPS_UPDATECOUNT'], type='float', alias='pps_updatecount') }},
        {{ trx_json_extract('src', ['PPS_PK'], type='float', alias='pps_pk') }},
        {{ trx_json_extract('src', ['PPS_LOCKED'], type='varchar', alias='pps_locked') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pmplansession') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pps_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pps_lastsaved']) }}
