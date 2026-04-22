{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BVDWWOACCUMSNAP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZOB_KEY'], type='float', alias='zob_key') }},
        {{ trx_json_extract('src', ['ZWO_KEY'], type='float', alias='zwo_key') }},
        {{ trx_json_extract('src', ['ZDT_KEY'], type='float', alias='zdt_key') }},
        {{ trx_json_extract('src', ['ZOR_KEY'], type='float', alias='zor_key') }},
        {{ trx_json_extract('src', ['ZMR_KEY'], type='float', alias='zmr_key') }},
        {{ trx_json_extract('src', ['ZWS_ACTDURATION'], type='float', alias='zws_actduration') }},
        {{ trx_json_extract('src', ['ZWS_ACTLABORCOSTDEFCUR'], type='float', alias='zws_actlaborcostdefcur') }},
        {{ trx_json_extract('src', ['ZWS_ACTLABORCOSTORGCUR'], type='float', alias='zws_actlaborcostorgcur') }},
        {{ trx_json_extract('src', ['ZWS_ACTLABORHOURS'], type='float', alias='zws_actlaborhours') }},
        {{ trx_json_extract('src', ['ZWS_ACTMATLCOSTDEFCUR'], type='float', alias='zws_actmatlcostdefcur') }},
        {{ trx_json_extract('src', ['ZWS_ACTMATLCOSTORGCUR'], type='float', alias='zws_actmatlcostorgcur') }},
        {{ trx_json_extract('src', ['ZWS_ACTUALCOMPDATEKEY'], type='float', alias='zws_actualcompdatekey') }},
        {{ trx_json_extract('src', ['ZWS_ACTUALSTARTDATEKEY'], type='float', alias='zws_actualstartdatekey') }},
        {{ trx_json_extract('src', ['ZWS_DEFCUR'], type='varchar', alias='zws_defcur') }},
        {{ trx_json_extract('src', ['ZWS_DOWNTIME'], type='float', alias='zws_downtime') }},
        {{ trx_json_extract('src', ['ZWS_DOWNTIMECOSTDEFCUR'], type='float', alias='zws_downtimecostdefcur') }},
        {{ trx_json_extract('src', ['ZWS_DOWNTIMECOSTORGCUR'], type='float', alias='zws_downtimecostorgcur') }},
        {{ trx_json_extract('src', ['ZWS_ESTLABORCOSTDEFCUR'], type='float', alias='zws_estlaborcostdefcur') }},
        {{ trx_json_extract('src', ['ZWS_ESTLABORCOSTORGCUR'], type='float', alias='zws_estlaborcostorgcur') }},
        {{ trx_json_extract('src', ['ZWS_ESTLABORHOURS'], type='float', alias='zws_estlaborhours') }},
        {{ trx_json_extract('src', ['ZWS_ESTMATLCOSTDEFCUR'], type='float', alias='zws_estmatlcostdefcur') }},
        {{ trx_json_extract('src', ['ZWS_ESTMATLCOSTORGCUR'], type='float', alias='zws_estmatlcostorgcur') }},
        {{ trx_json_extract('src', ['ZWS_ORGCUR'], type='varchar', alias='zws_orgcur') }},
        {{ trx_json_extract('src', ['ZWS_SCHEDCOMPDATEKEY'], type='float', alias='zws_schedcompdatekey') }},
        {{ trx_json_extract('src', ['ZWS_SCHEDDURATION'], type='float', alias='zws_schedduration') }},
        {{ trx_json_extract('src', ['ZWS_SCHEDULEDHOURS'], type='float', alias='zws_scheduledhours') }},
        {{ trx_json_extract('src', ['ZWS_SRDATEREQUESTEDKEY'], type='float', alias='zws_srdaterequestedkey') }},
        {{ trx_json_extract('src', ['ZWS_SRESTCOSTDEFCUR'], type='float', alias='zws_srestcostdefcur') }},
        {{ trx_json_extract('src', ['ZWS_SRESTCOSTORGCUR'], type='float', alias='zws_srestcostorgcur') }},
        {{ trx_json_extract('src', ['ZWS_SRKEY'], type='float', alias='zws_srkey') }},
        {{ trx_json_extract('src', ['ZWS_SRRESPONSETIME'], type='float', alias='zws_srresponsetime') }},
        {{ trx_json_extract('src', ['ZWS_WOCREATETOACTCOMPLAG'], type='float', alias='zws_wocreatetoactcomplag') }},
        {{ trx_json_extract('src', ['ZWS_WOCREATETOACTSTARTLAG'], type='float', alias='zws_wocreatetoactstartlag') }},
        {{ trx_json_extract('src', ['ZWS_WOCREATETOSCHEDSTARTLAG'], type='float', alias='zws_wocreatetoschedstartlag') }},
        {{ trx_json_extract('src', ['ZWS_WODATECREATEDKEY'], type='float', alias='zws_wodatecreatedkey') }},
        {{ trx_json_extract('src', ['ZWS_WOCODE'], type='varchar', alias='zws_wocode') }},
        {{ trx_json_extract('src', ['ZWS_LASTSAVED'], type='timestamp_ntz', alias='zws_lastsaved') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5bvdwwoaccumsnap') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_load_datetime']) }}
