{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TASKCHECKLISTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TCH_LASTSAVED'], type='timestamp_ntz', alias='tch_lastsaved') }},
        {{ trx_json_extract('src', ['TCH_METRICFRACTIONSLIDER'], type='varchar', alias='tch_metricfractionslider') }},
        {{ trx_json_extract('src', ['TCH_TASKREV'], type='float', alias='tch_taskrev') }},
        {{ trx_json_extract('src', ['TCH_DESC'], type='varchar', alias='tch_desc') }},
        {{ trx_json_extract('src', ['TCH_SEQUENCE'], type='float', alias='tch_sequence') }},
        {{ trx_json_extract('src', ['TCH_NOTUSED'], type='varchar', alias='tch_notused') }},
        {{ trx_json_extract('src', ['TCH_UPDATED'], type='timestamp_ntz', alias='tch_updated') }},
        {{ trx_json_extract('src', ['TCH_UPDATEDBY'], type='varchar', alias='tch_updatedby') }},
        {{ trx_json_extract('src', ['TCH_UPDATECOUNT'], type='float', alias='tch_updatecount') }},
        {{ trx_json_extract('src', ['TCH_TYPE'], type='varchar', alias='tch_type') }},
        {{ trx_json_extract('src', ['TCH_REQUIREDTOCLOSE'], type='varchar', alias='tch_requiredtoclose') }},
        {{ trx_json_extract('src', ['TCH_OBJECTLEVEL'], type='varchar', alias='tch_objectlevel') }},
        {{ trx_json_extract('src', ['TCH_OBJECTCLASS'], type='varchar', alias='tch_objectclass') }},
        {{ trx_json_extract('src', ['TCH_OBJECTCLASS_ORG'], type='varchar', alias='tch_objectclass_org') }},
        {{ trx_json_extract('src', ['TCH_OBJECTCATEGORY'], type='varchar', alias='tch_objectcategory') }},
        {{ trx_json_extract('src', ['TCH_UOM'], type='varchar', alias='tch_uom') }},
        {{ trx_json_extract('src', ['TCH_ASPECT'], type='varchar', alias='tch_aspect') }},
        {{ trx_json_extract('src', ['TCH_POINTTYPE'], type='varchar', alias='tch_pointtype') }},
        {{ trx_json_extract('src', ['TCH_REPEATING'], type='varchar', alias='tch_repeating') }},
        {{ trx_json_extract('src', ['TCH_FOLLOWUPTASK'], type='varchar', alias='tch_followuptask') }},
        {{ trx_json_extract('src', ['TCH_MATLIST'], type='varchar', alias='tch_matlist') }},
        {{ trx_json_extract('src', ['TCH_SYSLEVEL'], type='varchar', alias='tch_syslevel') }},
        {{ trx_json_extract('src', ['TCH_ASMLEVEL'], type='varchar', alias='tch_asmlevel') }},
        {{ trx_json_extract('src', ['TCH_COMPLEVEL'], type='varchar', alias='tch_complevel') }},
        {{ trx_json_extract('src', ['TCH_COMPLOCATION'], type='varchar', alias='tch_complocation') }},
        {{ trx_json_extract('src', ['TCH_CONDITION'], type='varchar', alias='tch_condition') }},
        {{ trx_json_extract('src', ['TCH_POSSIBLEFINDINGS'], type='varchar', alias='tch_possiblefindings') }},
        {{ trx_json_extract('src', ['TCH_FOLLOWUPJOB'], type='varchar', alias='tch_followupjob') }},
        {{ trx_json_extract('src', ['TCH_PART'], type='varchar', alias='tch_part') }},
        {{ trx_json_extract('src', ['TCH_PART_ORG'], type='varchar', alias='tch_part_org') }},
        {{ trx_json_extract('src', ['TCH_PARTCONDITION'], type='varchar', alias='tch_partcondition') }},
        {{ trx_json_extract('src', ['TCH_PARTCLASS'], type='varchar', alias='tch_partclass') }},
        {{ trx_json_extract('src', ['TCH_PARTCLASS_ORG'], type='varchar', alias='tch_partclass_org') }},
        {{ trx_json_extract('src', ['TCH_PARTCATEGORY'], type='varchar', alias='tch_partcategory') }},
        {{ trx_json_extract('src', ['TCH_COLOR'], type='varchar', alias='tch_color') }},
        {{ trx_json_extract('src', ['TCH_REFERENCE'], type='varchar', alias='tch_reference') }},
        {{ trx_json_extract('src', ['TCH_EQUIPFILTER'], type='varchar', alias='tch_equipfilter') }},
        {{ trx_json_extract('src', ['TCH_MINSLIDERVALUE'], type='float', alias='tch_minslidervalue') }},
        {{ trx_json_extract('src', ['TCH_MAXSLIDERVALUE'], type='float', alias='tch_maxslidervalue') }},
        {{ trx_json_extract('src', ['TCH_SRCCALCULATEDVALUE'], type='varchar', alias='tch_srccalculatedvalue') }},
        {{ trx_json_extract('src', ['TCH_FORMULA'], type='varchar', alias='tch_formula') }},
        {{ trx_json_extract('src', ['TCH_DIRECTION_OPTIONS'], type='varchar', alias='tch_direction_options') }},
        {{ trx_json_extract('src', ['TCH_NOT_APPLICABLE_OPTIONS'], type='varchar', alias='tch_not_applicable_options') }},
        {{ trx_json_extract('src', ['TCH_CONDITION_OPTIONS'], type='varchar', alias='tch_condition_options') }},
        {{ trx_json_extract('src', ['TCH_GROUP_LABEL'], type='varchar', alias='tch_group_label') }},
        {{ trx_json_extract('src', ['TCH_RESPONSIBILITY'], type='varchar', alias='tch_responsibility') }},
        {{ trx_json_extract('src', ['TCH_MEASUREMENTRESP'], type='varchar', alias='tch_measurementresp') }},
        {{ trx_json_extract('src', ['TCH_HIDEFOLLOWUP'], type='varchar', alias='tch_hidefollowup') }},
        {{ trx_json_extract('src', ['TCH_HIDELINEARFIELDS'], type='varchar', alias='tch_hidelinearfields') }},
        {{ trx_json_extract('src', ['TCH_ENABLENONCONFORMITIES'], type='varchar', alias='tch_enablenonconformities') }},
        {{ trx_json_extract('src', ['TCH_FRACTIONSLIDERDIMENSIONS'], type='varchar', alias='tch_fractionsliderdimensions') }},
        {{ trx_json_extract('src', ['TCH_RESULTRENTITY'], type='varchar', alias='tch_resultrentity') }},
        {{ trx_json_extract('src', ['TCH_RESULTENTITYCLASSOPTIONS'], type='varchar', alias='tch_resultentityclassoptions') }},
        {{ trx_json_extract('src', ['TCH_CODE'], type='varchar', alias='tch_code') }},
        {{ trx_json_extract('src', ['TCH_TASK'], type='varchar', alias='tch_task') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5taskchecklists') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['tch_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['tch_lastsaved']) }}
