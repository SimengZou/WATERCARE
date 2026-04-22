{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PMFORECASTPARAMS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PFP_LASTSAVED'], type='timestamp_ntz', alias='pfp_lastsaved') }},
        {{ trx_json_extract('src', ['PFP_PARAMETER'], type='varchar', alias='pfp_parameter') }},
        {{ trx_json_extract('src', ['PFP_DEFPARAM'], type='varchar', alias='pfp_defparam') }},
        {{ trx_json_extract('src', ['PFP_SESSIONID'], type='float', alias='pfp_sessionid') }},
        {{ trx_json_extract('src', ['PFP_STARTDATE'], type='timestamp_ntz', alias='pfp_startdate') }},
        {{ trx_json_extract('src', ['PFP_ENDDATE'], type='timestamp_ntz', alias='pfp_enddate') }},
        {{ trx_json_extract('src', ['PFP_OBJECTS'], type='varchar', alias='pfp_objects') }},
        {{ trx_json_extract('src', ['PFP_OBTYPES'], type='varchar', alias='pfp_obtypes') }},
        {{ trx_json_extract('src', ['PFP_OBCLASSES'], type='varchar', alias='pfp_obclasses') }},
        {{ trx_json_extract('src', ['PFP_TOPLEVEL'], type='varchar', alias='pfp_toplevel') }},
        {{ trx_json_extract('src', ['PFP_TOPLEVEL_ORG'], type='varchar', alias='pfp_toplevel_org') }},
        {{ trx_json_extract('src', ['PFP_CATEGORIES'], type='varchar', alias='pfp_categories') }},
        {{ trx_json_extract('src', ['PFP_CRITICALITIES'], type='varchar', alias='pfp_criticalities') }},
        {{ trx_json_extract('src', ['PFP_PMSCHEDULES'], type='varchar', alias='pfp_pmschedules') }},
        {{ trx_json_extract('src', ['PFP_PMCLASSES'], type='varchar', alias='pfp_pmclasses') }},
        {{ trx_json_extract('src', ['PFP_NESTED'], type='varchar', alias='pfp_nested') }},
        {{ trx_json_extract('src', ['PFP_PRIORITIES'], type='varchar', alias='pfp_priorities') }},
        {{ trx_json_extract('src', ['PFP_JOBTYPES'], type='varchar', alias='pfp_jobtypes') }},
        {{ trx_json_extract('src', ['PFP_EVENT_ORG'], type='varchar', alias='pfp_event_org') }},
        {{ trx_json_extract('src', ['PFP_EVENT_CLASSES'], type='varchar', alias='pfp_event_classes') }},
        {{ trx_json_extract('src', ['PFP_MRCS'], type='varchar', alias='pfp_mrcs') }},
        {{ trx_json_extract('src', ['PFP_LOCATIONS'], type='varchar', alias='pfp_locations') }},
        {{ trx_json_extract('src', ['PFP_PERSONS'], type='varchar', alias='pfp_persons') }},
        {{ trx_json_extract('src', ['PFP_COSTCODES'], type='varchar', alias='pfp_costcodes') }},
        {{ trx_json_extract('src', ['PFP_SCHEDGRPS'], type='varchar', alias='pfp_schedgrps') }},
        {{ trx_json_extract('src', ['PFP_PARENTTYPE'], type='varchar', alias='pfp_parenttype') }},
        {{ trx_json_extract('src', ['PFP_WORKHOURS'], type='float', alias='pfp_workhours') }},
        {{ trx_json_extract('src', ['PFP_MIN_FREQ'], type='float', alias='pfp_min_freq') }},
        {{ trx_json_extract('src', ['PFP_INC_CHILDREN'], type='varchar', alias='pfp_inc_children') }},
        {{ trx_json_extract('src', ['PFP_WORKORDER_BGCOLOR'], type='varchar', alias='pfp_workorder_bgcolor') }},
        {{ trx_json_extract('src', ['PFP_ACTDUEDT_BGCOLOR'], type='varchar', alias='pfp_actduedt_bgcolor') }},
        {{ trx_json_extract('src', ['PFP_FORECASTPM_BGCOLOR'], type='varchar', alias='pfp_forecastpm_bgcolor') }},
        {{ trx_json_extract('src', ['PFP_WEEKEND_BGCOLOR'], type='varchar', alias='pfp_weekend_bgcolor') }},
        {{ trx_json_extract('src', ['PFP_LKDPMDUEDT_TXTCOLOR'], type='varchar', alias='pfp_lkdpmduedt_txtcolor') }},
        {{ trx_json_extract('src', ['PFP_CAL_DAY'], type='varchar', alias='pfp_cal_day') }},
        {{ trx_json_extract('src', ['PFP_YEAR_DSG'], type='varchar', alias='pfp_year_dsg') }},
        {{ trx_json_extract('src', ['PFP_QTR_DSG'], type='varchar', alias='pfp_qtr_dsg') }},
        {{ trx_json_extract('src', ['PFP_MONTH_DSG'], type='varchar', alias='pfp_month_dsg') }},
        {{ trx_json_extract('src', ['PFP_WEEK_DSG'], type='varchar', alias='pfp_week_dsg') }},
        {{ trx_json_extract('src', ['PFP_DAILY_DSG'], type='varchar', alias='pfp_daily_dsg') }},
        {{ trx_json_extract('src', ['PFP_BACKFILLING'], type='varchar', alias='pfp_backfilling') }},
        {{ trx_json_extract('src', ['PFP_FORWARDFILLING'], type='varchar', alias='pfp_forwardfilling') }},
        {{ trx_json_extract('src', ['PFP_SESSION_APRV'], type='varchar', alias='pfp_session_aprv') }},
        {{ trx_json_extract('src', ['PFP_FORECASTING'], type='varchar', alias='pfp_forecasting') }},
        {{ trx_json_extract('src', ['PFP_CREATED'], type='timestamp_ntz', alias='pfp_created') }},
        {{ trx_json_extract('src', ['PFP_ENABLECHILDEQUIPTAB'], type='varchar', alias='pfp_enablechildequiptab') }},
        {{ trx_json_extract('src', ['PFP_SCREENHSPLIT'], type='float', alias='pfp_screenhsplit') }},
        {{ trx_json_extract('src', ['PFP_PAGESIZE'], type='float', alias='pfp_pagesize') }},
        {{ trx_json_extract('src', ['PFP_UPDATECOUNT'], type='float', alias='pfp_updatecount') }},
        {{ trx_json_extract('src', ['PFP_CODE'], type='varchar', alias='pfp_code') }},
        {{ trx_json_extract('src', ['PFP_USER'], type='varchar', alias='pfp_user') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pmforecastparams') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pfp_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pfp_lastsaved']) }}
