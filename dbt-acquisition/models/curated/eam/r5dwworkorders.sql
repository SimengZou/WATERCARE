{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWWORKORDERS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZWO_LASTSAVED'], type='timestamp_ntz', alias='zwo_lastsaved') }},
        {{ trx_json_extract('src', ['ZWO_CODE'], type='varchar', alias='zwo_code') }},
        {{ trx_json_extract('src', ['ZWO_DESC'], type='varchar', alias='zwo_desc') }},
        {{ trx_json_extract('src', ['ZWO_TYPECODE'], type='varchar', alias='zwo_typecode') }},
        {{ trx_json_extract('src', ['ZWO_TYPEDESC'], type='varchar', alias='zwo_typedesc') }},
        {{ trx_json_extract('src', ['ZWO_JOBTYPECODE'], type='varchar', alias='zwo_jobtypecode') }},
        {{ trx_json_extract('src', ['ZWO_JOBTYPEDESC'], type='varchar', alias='zwo_jobtypedesc') }},
        {{ trx_json_extract('src', ['ZWO_CLASSCODE'], type='varchar', alias='zwo_classcode') }},
        {{ trx_json_extract('src', ['ZWO_CLASSORG'], type='varchar', alias='zwo_classorg') }},
        {{ trx_json_extract('src', ['ZWO_CLASSDESC'], type='varchar', alias='zwo_classdesc') }},
        {{ trx_json_extract('src', ['ZWO_STATUSCODE'], type='varchar', alias='zwo_statuscode') }},
        {{ trx_json_extract('src', ['ZWO_STATUSDESC'], type='varchar', alias='zwo_statusdesc') }},
        {{ trx_json_extract('src', ['ZWO_COSTCODE'], type='varchar', alias='zwo_costcode') }},
        {{ trx_json_extract('src', ['ZWO_COSTCODEDESC'], type='varchar', alias='zwo_costcodedesc') }},
        {{ trx_json_extract('src', ['ZWO_LOCATIONCODE'], type='varchar', alias='zwo_locationcode') }},
        {{ trx_json_extract('src', ['ZWO_LOCATIONORG'], type='varchar', alias='zwo_locationorg') }},
        {{ trx_json_extract('src', ['ZWO_LOCATIONDESC'], type='varchar', alias='zwo_locationdesc') }},
        {{ trx_json_extract('src', ['ZWO_PROJECTCODE'], type='varchar', alias='zwo_projectcode') }},
        {{ trx_json_extract('src', ['ZWO_PROJECTDESC'], type='varchar', alias='zwo_projectdesc') }},
        {{ trx_json_extract('src', ['ZWO_PROJBUDGETCODE'], type='varchar', alias='zwo_projbudgetcode') }},
        {{ trx_json_extract('src', ['ZWO_PRIORITY'], type='varchar', alias='zwo_priority') }},
        {{ trx_json_extract('src', ['ZWO_SHIFTCODE'], type='varchar', alias='zwo_shiftcode') }},
        {{ trx_json_extract('src', ['ZWO_SHIFTDESC'], type='varchar', alias='zwo_shiftdesc') }},
        {{ trx_json_extract('src', ['ZWO_ASSIGNEDBYCODE'], type='varchar', alias='zwo_assignedbycode') }},
        {{ trx_json_extract('src', ['ZWO_ASSIGNEDBYNAME'], type='varchar', alias='zwo_assignedbyname') }},
        {{ trx_json_extract('src', ['ZWO_ASSIGNEDTOCODE'], type='varchar', alias='zwo_assignedtocode') }},
        {{ trx_json_extract('src', ['ZWO_ASSIGNEDTONAME'], type='varchar', alias='zwo_assignedtoname') }},
        {{ trx_json_extract('src', ['ZWO_OBJUNDERWARRANTY'], type='varchar', alias='zwo_objunderwarranty') }},
        {{ trx_json_extract('src', ['ZWO_REOPENED'], type='varchar', alias='zwo_reopened') }},
        {{ trx_json_extract('src', ['ZWO_PROBLEMCODE'], type='varchar', alias='zwo_problemcode') }},
        {{ trx_json_extract('src', ['ZWO_PROBLEMDESC'], type='varchar', alias='zwo_problemdesc') }},
        {{ trx_json_extract('src', ['ZWO_FAILURECODE'], type='varchar', alias='zwo_failurecode') }},
        {{ trx_json_extract('src', ['ZWO_FAILUREDESC'], type='varchar', alias='zwo_failuredesc') }},
        {{ trx_json_extract('src', ['ZWO_CAUSECODE'], type='varchar', alias='zwo_causecode') }},
        {{ trx_json_extract('src', ['ZWO_CAUSEDESC'], type='varchar', alias='zwo_causedesc') }},
        {{ trx_json_extract('src', ['ZWO_ACTIONCODE'], type='varchar', alias='zwo_actioncode') }},
        {{ trx_json_extract('src', ['ZWO_ACTIONDESC'], type='varchar', alias='zwo_actiondesc') }},
        {{ trx_json_extract('src', ['ZWO_RTYPECODE'], type='varchar', alias='zwo_rtypecode') }},
        {{ trx_json_extract('src', ['ZWO_RTYPEDESC'], type='varchar', alias='zwo_rtypedesc') }},
        {{ trx_json_extract('src', ['ZWO_RSTATUSCODE'], type='varchar', alias='zwo_rstatuscode') }},
        {{ trx_json_extract('src', ['ZWO_RSTATUSDESC'], type='varchar', alias='zwo_rstatusdesc') }},
        {{ trx_json_extract('src', ['ZWO_KEY'], type='float', alias='zwo_key') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwworkorders') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['zwo_lastsaved']) }}
