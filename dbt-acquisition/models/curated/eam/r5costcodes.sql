{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5COSTCODES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CST_LASTSAVED'], type='timestamp_ntz', alias='cst_lastsaved') }},
        {{ trx_json_extract('src', ['CST_UDFCHKBOX05'], type='varchar', alias='cst_udfchkbox05') }},
        {{ trx_json_extract('src', ['CST_CLASS'], type='varchar', alias='cst_class') }},
        {{ trx_json_extract('src', ['CST_NOTUSED'], type='varchar', alias='cst_notused') }},
        {{ trx_json_extract('src', ['CST_ORG'], type='varchar', alias='cst_org') }},
        {{ trx_json_extract('src', ['CST_CLASS_ORG'], type='varchar', alias='cst_class_org') }},
        {{ trx_json_extract('src', ['CST_CREATED'], type='timestamp_ntz', alias='cst_created') }},
        {{ trx_json_extract('src', ['CST_CREATEDBY'], type='varchar', alias='cst_createdby') }},
        {{ trx_json_extract('src', ['CST_UPDATED'], type='timestamp_ntz', alias='cst_updated') }},
        {{ trx_json_extract('src', ['CST_UPDATEDBY'], type='varchar', alias='cst_updatedby') }},
        {{ trx_json_extract('src', ['CST_UPDATECOUNT'], type='float', alias='cst_updatecount') }},
        {{ trx_json_extract('src', ['CST_FLEETCUSTOMER'], type='varchar', alias='cst_fleetcustomer') }},
        {{ trx_json_extract('src', ['CST_FLEETCUSTOMER_ORG'], type='varchar', alias='cst_fleetcustomer_org') }},
        {{ trx_json_extract('src', ['CST_NONBILLABLE'], type='varchar', alias='cst_nonbillable') }},
        {{ trx_json_extract('src', ['CST_SEGMENTVALUE'], type='varchar', alias='cst_segmentvalue') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR01'], type='varchar', alias='cst_udfchar01') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR02'], type='varchar', alias='cst_udfchar02') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR03'], type='varchar', alias='cst_udfchar03') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR04'], type='varchar', alias='cst_udfchar04') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR05'], type='varchar', alias='cst_udfchar05') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR06'], type='varchar', alias='cst_udfchar06') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR07'], type='varchar', alias='cst_udfchar07') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR08'], type='varchar', alias='cst_udfchar08') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR09'], type='varchar', alias='cst_udfchar09') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR10'], type='varchar', alias='cst_udfchar10') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR12'], type='varchar', alias='cst_udfchar12') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR13'], type='varchar', alias='cst_udfchar13') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR14'], type='varchar', alias='cst_udfchar14') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR15'], type='varchar', alias='cst_udfchar15') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR16'], type='varchar', alias='cst_udfchar16') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR17'], type='varchar', alias='cst_udfchar17') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR18'], type='varchar', alias='cst_udfchar18') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR19'], type='varchar', alias='cst_udfchar19') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR20'], type='varchar', alias='cst_udfchar20') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR21'], type='varchar', alias='cst_udfchar21') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR22'], type='varchar', alias='cst_udfchar22') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR23'], type='varchar', alias='cst_udfchar23') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR24'], type='varchar', alias='cst_udfchar24') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR25'], type='varchar', alias='cst_udfchar25') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR26'], type='varchar', alias='cst_udfchar26') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR27'], type='varchar', alias='cst_udfchar27') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR28'], type='varchar', alias='cst_udfchar28') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR29'], type='varchar', alias='cst_udfchar29') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR30'], type='varchar', alias='cst_udfchar30') }},
        {{ trx_json_extract('src', ['CST_UDFNUM01'], type='float', alias='cst_udfnum01') }},
        {{ trx_json_extract('src', ['CST_UDFCHAR11'], type='varchar', alias='cst_udfchar11') }},
        {{ trx_json_extract('src', ['CST_UDFNUM02'], type='float', alias='cst_udfnum02') }},
        {{ trx_json_extract('src', ['CST_UDFNUM03'], type='float', alias='cst_udfnum03') }},
        {{ trx_json_extract('src', ['CST_UDFNUM04'], type='float', alias='cst_udfnum04') }},
        {{ trx_json_extract('src', ['CST_UDFNUM05'], type='float', alias='cst_udfnum05') }},
        {{ trx_json_extract('src', ['CST_UDFDATE01'], type='timestamp_ntz', alias='cst_udfdate01') }},
        {{ trx_json_extract('src', ['CST_UDFDATE02'], type='timestamp_ntz', alias='cst_udfdate02') }},
        {{ trx_json_extract('src', ['CST_UDFDATE03'], type='timestamp_ntz', alias='cst_udfdate03') }},
        {{ trx_json_extract('src', ['CST_UDFDATE04'], type='timestamp_ntz', alias='cst_udfdate04') }},
        {{ trx_json_extract('src', ['CST_UDFDATE05'], type='timestamp_ntz', alias='cst_udfdate05') }},
        {{ trx_json_extract('src', ['CST_UDFCHKBOX01'], type='varchar', alias='cst_udfchkbox01') }},
        {{ trx_json_extract('src', ['CST_UDFCHKBOX02'], type='varchar', alias='cst_udfchkbox02') }},
        {{ trx_json_extract('src', ['CST_UDFCHKBOX03'], type='varchar', alias='cst_udfchkbox03') }},
        {{ trx_json_extract('src', ['CST_UDFCHKBOX04'], type='varchar', alias='cst_udfchkbox04') }},
        {{ trx_json_extract('src', ['CST_CODE'], type='varchar', alias='cst_code') }},
        {{ trx_json_extract('src', ['CST_DESC'], type='varchar', alias='cst_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5costcodes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cst_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cst_lastsaved']) }}
