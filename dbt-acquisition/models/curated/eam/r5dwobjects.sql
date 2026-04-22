{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWOBJECTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZOB_LASTSAVED'], type='timestamp_ntz', alias='zob_lastsaved') }},
        {{ trx_json_extract('src', ['ZOB_VARIABLE3'], type='varchar', alias='zob_variable3') }},
        {{ trx_json_extract('src', ['ZOB_VARIABLE4'], type='varchar', alias='zob_variable4') }},
        {{ trx_json_extract('src', ['ZOB_VARIABLE5'], type='varchar', alias='zob_variable5') }},
        {{ trx_json_extract('src', ['ZOB_VARIABLE6'], type='varchar', alias='zob_variable6') }},
        {{ trx_json_extract('src', ['ZOB_KEY'], type='float', alias='zob_key') }},
        {{ trx_json_extract('src', ['ZOB_CODE'], type='varchar', alias='zob_code') }},
        {{ trx_json_extract('src', ['ZOB_DESC'], type='varchar', alias='zob_desc') }},
        {{ trx_json_extract('src', ['ZOB_ORGCODE'], type='varchar', alias='zob_orgcode') }},
        {{ trx_json_extract('src', ['ZOB_ORGDESC'], type='varchar', alias='zob_orgdesc') }},
        {{ trx_json_extract('src', ['ZOB_TYPECODE'], type='varchar', alias='zob_typecode') }},
        {{ trx_json_extract('src', ['ZOB_TYPEDESC'], type='varchar', alias='zob_typedesc') }},
        {{ trx_json_extract('src', ['ZOB_CLASSCODE'], type='varchar', alias='zob_classcode') }},
        {{ trx_json_extract('src', ['ZOB_CLASSORG'], type='varchar', alias='zob_classorg') }},
        {{ trx_json_extract('src', ['ZOB_CLASSDESC'], type='varchar', alias='zob_classdesc') }},
        {{ trx_json_extract('src', ['ZOB_LOCATIONCODE'], type='varchar', alias='zob_locationcode') }},
        {{ trx_json_extract('src', ['ZOB_LOCATIONORG'], type='varchar', alias='zob_locationorg') }},
        {{ trx_json_extract('src', ['ZOB_LOCATIONDESC'], type='varchar', alias='zob_locationdesc') }},
        {{ trx_json_extract('src', ['ZOB_MANUFACTURERCODE'], type='varchar', alias='zob_manufacturercode') }},
        {{ trx_json_extract('src', ['ZOB_MANUFACTURERDESC'], type='varchar', alias='zob_manufacturerdesc') }},
        {{ trx_json_extract('src', ['ZOB_COSTCODE'], type='varchar', alias='zob_costcode') }},
        {{ trx_json_extract('src', ['ZOB_COSTCODEDESC'], type='varchar', alias='zob_costcodedesc') }},
        {{ trx_json_extract('src', ['ZOB_STATUSCODE'], type='varchar', alias='zob_statuscode') }},
        {{ trx_json_extract('src', ['ZOB_STATUSDESC'], type='varchar', alias='zob_statusdesc') }},
        {{ trx_json_extract('src', ['ZOB_STATECODE'], type='varchar', alias='zob_statecode') }},
        {{ trx_json_extract('src', ['ZOB_STATEDESC'], type='varchar', alias='zob_statedesc') }},
        {{ trx_json_extract('src', ['ZOB_PRODUCTIONRELATED'], type='varchar', alias='zob_productionrelated') }},
        {{ trx_json_extract('src', ['ZOB_SAFETYRELATED'], type='varchar', alias='zob_safetyrelated') }},
        {{ trx_json_extract('src', ['ZOB_CRITICALITY'], type='varchar', alias='zob_criticality') }},
        {{ trx_json_extract('src', ['ZOB_VALUEDEFCUR'], type='float', alias='zob_valuedefcur') }},
        {{ trx_json_extract('src', ['ZOB_DEFCUR'], type='varchar', alias='zob_defcur') }},
        {{ trx_json_extract('src', ['ZOB_VALUEORGCUR'], type='float', alias='zob_valueorgcur') }},
        {{ trx_json_extract('src', ['ZOB_ORGCUR'], type='varchar', alias='zob_orgcur') }},
        {{ trx_json_extract('src', ['ZOB_ASSIGNEDTOCODE'], type='varchar', alias='zob_assignedtocode') }},
        {{ trx_json_extract('src', ['ZOB_ASSIGNEDTONAME'], type='varchar', alias='zob_assignedtoname') }},
        {{ trx_json_extract('src', ['ZOB_COMMISSIONDATE'], type='timestamp_ntz', alias='zob_commissiondate') }},
        {{ trx_json_extract('src', ['ZOB_WITHDRAWALDATE'], type='timestamp_ntz', alias='zob_withdrawaldate') }},
        {{ trx_json_extract('src', ['ZOB_CATEGORY'], type='varchar', alias='zob_category') }},
        {{ trx_json_extract('src', ['ZOB_CATEGORYDESC'], type='varchar', alias='zob_categorydesc') }},
        {{ trx_json_extract('src', ['ZOB_RTYPECODE'], type='varchar', alias='zob_rtypecode') }},
        {{ trx_json_extract('src', ['ZOB_RTYPEDESC'], type='varchar', alias='zob_rtypedesc') }},
        {{ trx_json_extract('src', ['ZOB_RSTATUSCODE'], type='varchar', alias='zob_rstatuscode') }},
        {{ trx_json_extract('src', ['ZOB_RSTATUSDESC'], type='varchar', alias='zob_rstatusdesc') }},
        {{ trx_json_extract('src', ['ZOB_RSTATECODE'], type='varchar', alias='zob_rstatecode') }},
        {{ trx_json_extract('src', ['ZOB_RSTATEDESC'], type='varchar', alias='zob_rstatedesc') }},
        {{ trx_json_extract('src', ['ZOB_VARIABLE1'], type='varchar', alias='zob_variable1') }},
        {{ trx_json_extract('src', ['ZOB_VARIABLE2'], type='varchar', alias='zob_variable2') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwobjects') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['zob_lastsaved']) }}
