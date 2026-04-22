{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5HOME',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['HOM_LASTSAVED'], type='timestamp_ntz', alias='hom_lastsaved') }},
        {{ trx_json_extract('src', ['HOM_DESC'], type='varchar', alias='hom_desc') }},
        {{ trx_json_extract('src', ['HOM_SQLCODE'], type='varchar', alias='hom_sqlcode') }},
        {{ trx_json_extract('src', ['HOM_FUNCTION'], type='varchar', alias='hom_function') }},
        {{ trx_json_extract('src', ['HOM_GEN'], type='varchar', alias='hom_gen') }},
        {{ trx_json_extract('src', ['HOM_PARENT'], type='varchar', alias='hom_parent') }},
        {{ trx_json_extract('src', ['HOM_UPDFREQ'], type='float', alias='hom_updfreq') }},
        {{ trx_json_extract('src', ['HOM_UPDATE'], type='timestamp_ntz', alias='hom_update') }},
        {{ trx_json_extract('src', ['HOM_NXTUPDATE'], type='timestamp_ntz', alias='hom_nxtupdate') }},
        {{ trx_json_extract('src', ['HOM_UOM'], type='varchar', alias='hom_uom') }},
        {{ trx_json_extract('src', ['HOM_HISTORY'], type='varchar', alias='hom_history') }},
        {{ trx_json_extract('src', ['HOM_WHERECLAUSE'], type='varchar', alias='hom_whereclause') }},
        {{ trx_json_extract('src', ['HOM_CURVALUE'], type='float', alias='hom_curvalue') }},
        {{ trx_json_extract('src', ['HOM_NORMSCORE'], type='float', alias='hom_normscore') }},
        {{ trx_json_extract('src', ['HOM_ERROR'], type='varchar', alias='hom_error') }},
        {{ trx_json_extract('src', ['HOM_UPDATECOUNT'], type='float', alias='hom_updatecount') }},
        {{ trx_json_extract('src', ['HOM_EWSFUNCTION'], type='varchar', alias='hom_ewsfunction') }},
        {{ trx_json_extract('src', ['HOM_EWSDATASPYID'], type='float', alias='hom_ewsdataspyid') }},
        {{ trx_json_extract('src', ['HOM_OPERATOR'], type='varchar', alias='hom_operator') }},
        {{ trx_json_extract('src', ['HOM_VALUE'], type='float', alias='hom_value') }},
        {{ trx_json_extract('src', ['HOM_NOTUSED'], type='varchar', alias='hom_notused') }},
        {{ trx_json_extract('src', ['HOM_ROLLUPTYPE'], type='varchar', alias='hom_rolluptype') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR01'], type='varchar', alias='hom_udfchar01') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR02'], type='varchar', alias='hom_udfchar02') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR03'], type='varchar', alias='hom_udfchar03') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR04'], type='varchar', alias='hom_udfchar04') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR05'], type='varchar', alias='hom_udfchar05') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR06'], type='varchar', alias='hom_udfchar06') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR07'], type='varchar', alias='hom_udfchar07') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR08'], type='varchar', alias='hom_udfchar08') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR09'], type='varchar', alias='hom_udfchar09') }},
        {{ trx_json_extract('src', ['HOM_UDFCHAR10'], type='varchar', alias='hom_udfchar10') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM01'], type='float', alias='hom_udfnum01') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM02'], type='float', alias='hom_udfnum02') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM03'], type='float', alias='hom_udfnum03') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM04'], type='float', alias='hom_udfnum04') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM05'], type='float', alias='hom_udfnum05') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM06'], type='float', alias='hom_udfnum06') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM07'], type='float', alias='hom_udfnum07') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM08'], type='float', alias='hom_udfnum08') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM09'], type='float', alias='hom_udfnum09') }},
        {{ trx_json_extract('src', ['HOM_UDFNUM10'], type='float', alias='hom_udfnum10') }},
        {{ trx_json_extract('src', ['HOM_UDFDATE01'], type='timestamp_ntz', alias='hom_udfdate01') }},
        {{ trx_json_extract('src', ['HOM_UDFDATE02'], type='timestamp_ntz', alias='hom_udfdate02') }},
        {{ trx_json_extract('src', ['HOM_UDFDATE03'], type='timestamp_ntz', alias='hom_udfdate03') }},
        {{ trx_json_extract('src', ['HOM_UDFDATE04'], type='timestamp_ntz', alias='hom_udfdate04') }},
        {{ trx_json_extract('src', ['HOM_UDFDATE05'], type='timestamp_ntz', alias='hom_udfdate05') }},
        {{ trx_json_extract('src', ['HOM_UDFCHKBOX01'], type='varchar', alias='hom_udfchkbox01') }},
        {{ trx_json_extract('src', ['HOM_UDFCHKBOX02'], type='varchar', alias='hom_udfchkbox02') }},
        {{ trx_json_extract('src', ['HOM_UDFCHKBOX03'], type='varchar', alias='hom_udfchkbox03') }},
        {{ trx_json_extract('src', ['HOM_UDFCHKBOX04'], type='varchar', alias='hom_udfchkbox04') }},
        {{ trx_json_extract('src', ['HOM_UDFCHKBOX05'], type='varchar', alias='hom_udfchkbox05') }},
        {{ trx_json_extract('src', ['HOM_MIN'], type='float', alias='hom_min') }},
        {{ trx_json_extract('src', ['HOM_MAX'], type='float', alias='hom_max') }},
        {{ trx_json_extract('src', ['HOM_RADIUSPERCENTAGE'], type='float', alias='hom_radiuspercentage') }},
        {{ trx_json_extract('src', ['HOM_KPITYPE'], type='varchar', alias='hom_kpitype') }},
        {{ trx_json_extract('src', ['HOM_GROUPBYTEXT'], type='varchar', alias='hom_groupbytext') }},
        {{ trx_json_extract('src', ['HOM_CODE'], type='varchar', alias='hom_code') }},
        {{ trx_json_extract('src', ['HOM_TYPE'], type='varchar', alias='hom_type') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5home') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['hom_code', 'hom_type']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['hom_lastsaved']) }}
