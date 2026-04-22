{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BINSTOCK',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['BIS_LASTSAVED'], type='timestamp_ntz', alias='bis_lastsaved') }},
        {{ trx_json_extract('src', ['BIS_BIN'], type='varchar', alias='bis_bin') }},
        {{ trx_json_extract('src', ['BIS_LOT'], type='varchar', alias='bis_lot') }},
        {{ trx_json_extract('src', ['BIS_QTY'], type='float', alias='bis_qty') }},
        {{ trx_json_extract('src', ['BIS_PART_ORG'], type='varchar', alias='bis_part_org') }},
        {{ trx_json_extract('src', ['BIS_REPAIRQTY'], type='float', alias='bis_repairqty') }},
        {{ trx_json_extract('src', ['BIS_CREATED'], type='timestamp_ntz', alias='bis_created') }},
        {{ trx_json_extract('src', ['BIS_CREATEDBY'], type='varchar', alias='bis_createdby') }},
        {{ trx_json_extract('src', ['BIS_UPDATED'], type='timestamp_ntz', alias='bis_updated') }},
        {{ trx_json_extract('src', ['BIS_UPDATEDBY'], type='varchar', alias='bis_updatedby') }},
        {{ trx_json_extract('src', ['BIS_UPDATECOUNT'], type='float', alias='bis_updatecount') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR01'], type='varchar', alias='bis_udfchar01') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR02'], type='varchar', alias='bis_udfchar02') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR03'], type='varchar', alias='bis_udfchar03') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR04'], type='varchar', alias='bis_udfchar04') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR05'], type='varchar', alias='bis_udfchar05') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR06'], type='varchar', alias='bis_udfchar06') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR07'], type='varchar', alias='bis_udfchar07') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR08'], type='varchar', alias='bis_udfchar08') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR09'], type='varchar', alias='bis_udfchar09') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR10'], type='varchar', alias='bis_udfchar10') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR11'], type='varchar', alias='bis_udfchar11') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR12'], type='varchar', alias='bis_udfchar12') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR13'], type='varchar', alias='bis_udfchar13') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR14'], type='varchar', alias='bis_udfchar14') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR15'], type='varchar', alias='bis_udfchar15') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR16'], type='varchar', alias='bis_udfchar16') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR17'], type='varchar', alias='bis_udfchar17') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR18'], type='varchar', alias='bis_udfchar18') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR19'], type='varchar', alias='bis_udfchar19') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR20'], type='varchar', alias='bis_udfchar20') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR21'], type='varchar', alias='bis_udfchar21') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR22'], type='varchar', alias='bis_udfchar22') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR23'], type='varchar', alias='bis_udfchar23') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR24'], type='varchar', alias='bis_udfchar24') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR25'], type='varchar', alias='bis_udfchar25') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR26'], type='varchar', alias='bis_udfchar26') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR27'], type='varchar', alias='bis_udfchar27') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR28'], type='varchar', alias='bis_udfchar28') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR29'], type='varchar', alias='bis_udfchar29') }},
        {{ trx_json_extract('src', ['BIS_UDFCHAR30'], type='varchar', alias='bis_udfchar30') }},
        {{ trx_json_extract('src', ['BIS_UDFNUM01'], type='float', alias='bis_udfnum01') }},
        {{ trx_json_extract('src', ['BIS_UDFNUM02'], type='float', alias='bis_udfnum02') }},
        {{ trx_json_extract('src', ['BIS_UDFNUM03'], type='float', alias='bis_udfnum03') }},
        {{ trx_json_extract('src', ['BIS_UDFNUM04'], type='float', alias='bis_udfnum04') }},
        {{ trx_json_extract('src', ['BIS_UDFNUM05'], type='float', alias='bis_udfnum05') }},
        {{ trx_json_extract('src', ['BIS_UDFDATE01'], type='timestamp_ntz', alias='bis_udfdate01') }},
        {{ trx_json_extract('src', ['BIS_UDFDATE02'], type='timestamp_ntz', alias='bis_udfdate02') }},
        {{ trx_json_extract('src', ['BIS_UDFDATE03'], type='timestamp_ntz', alias='bis_udfdate03') }},
        {{ trx_json_extract('src', ['BIS_UDFDATE04'], type='timestamp_ntz', alias='bis_udfdate04') }},
        {{ trx_json_extract('src', ['BIS_UDFDATE05'], type='timestamp_ntz', alias='bis_udfdate05') }},
        {{ trx_json_extract('src', ['BIS_UDFCHKBOX01'], type='varchar', alias='bis_udfchkbox01') }},
        {{ trx_json_extract('src', ['BIS_UDFCHKBOX02'], type='varchar', alias='bis_udfchkbox02') }},
        {{ trx_json_extract('src', ['BIS_UDFCHKBOX03'], type='varchar', alias='bis_udfchkbox03') }},
        {{ trx_json_extract('src', ['BIS_UDFCHKBOX04'], type='varchar', alias='bis_udfchkbox04') }},
        {{ trx_json_extract('src', ['BIS_UDFCHKBOX05'], type='varchar', alias='bis_udfchkbox05') }},
        {{ trx_json_extract('src', ['BIS_PART'], type='varchar', alias='bis_part') }},
        {{ trx_json_extract('src', ['BIS_STORE'], type='varchar', alias='bis_store') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5binstock') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['bis_store', 'bis_part', 'bis_part_org', 'bis_bin', 'bis_lot']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['bis_lastsaved']) }}
