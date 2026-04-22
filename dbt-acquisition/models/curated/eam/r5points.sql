{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5POINTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['POI_LASTSAVED'], type='timestamp_ntz', alias='poi_lastsaved') }},
        {{ trx_json_extract('src', ['POI_OBTYPE'], type='varchar', alias='poi_obtype') }},
        {{ trx_json_extract('src', ['POI_POINT'], type='varchar', alias='poi_point') }},
        {{ trx_json_extract('src', ['POI_DESC'], type='varchar', alias='poi_desc') }},
        {{ trx_json_extract('src', ['POI_POINTTYPE'], type='varchar', alias='poi_pointtype') }},
        {{ trx_json_extract('src', ['POI_OBJECT_ORG'], type='varchar', alias='poi_object_org') }},
        {{ trx_json_extract('src', ['POI_UPDATECOUNT'], type='float', alias='poi_updatecount') }},
        {{ trx_json_extract('src', ['POI_CREATED'], type='timestamp_ntz', alias='poi_created') }},
        {{ trx_json_extract('src', ['POI_UPDATED'], type='timestamp_ntz', alias='poi_updated') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR01'], type='varchar', alias='poi_udfchar01') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR02'], type='varchar', alias='poi_udfchar02') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR03'], type='varchar', alias='poi_udfchar03') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR04'], type='varchar', alias='poi_udfchar04') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR05'], type='varchar', alias='poi_udfchar05') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR06'], type='varchar', alias='poi_udfchar06') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR07'], type='varchar', alias='poi_udfchar07') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR08'], type='varchar', alias='poi_udfchar08') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR09'], type='varchar', alias='poi_udfchar09') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR10'], type='varchar', alias='poi_udfchar10') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR11'], type='varchar', alias='poi_udfchar11') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR12'], type='varchar', alias='poi_udfchar12') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR13'], type='varchar', alias='poi_udfchar13') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR14'], type='varchar', alias='poi_udfchar14') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR15'], type='varchar', alias='poi_udfchar15') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR16'], type='varchar', alias='poi_udfchar16') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR17'], type='varchar', alias='poi_udfchar17') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR18'], type='varchar', alias='poi_udfchar18') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR19'], type='varchar', alias='poi_udfchar19') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR20'], type='varchar', alias='poi_udfchar20') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR21'], type='varchar', alias='poi_udfchar21') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR22'], type='varchar', alias='poi_udfchar22') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR23'], type='varchar', alias='poi_udfchar23') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR24'], type='varchar', alias='poi_udfchar24') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR25'], type='varchar', alias='poi_udfchar25') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR26'], type='varchar', alias='poi_udfchar26') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR27'], type='varchar', alias='poi_udfchar27') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR28'], type='varchar', alias='poi_udfchar28') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR29'], type='varchar', alias='poi_udfchar29') }},
        {{ trx_json_extract('src', ['POI_UDFCHAR30'], type='varchar', alias='poi_udfchar30') }},
        {{ trx_json_extract('src', ['POI_UDFNUM01'], type='float', alias='poi_udfnum01') }},
        {{ trx_json_extract('src', ['POI_UDFNUM02'], type='float', alias='poi_udfnum02') }},
        {{ trx_json_extract('src', ['POI_UDFNUM03'], type='float', alias='poi_udfnum03') }},
        {{ trx_json_extract('src', ['POI_UDFNUM04'], type='float', alias='poi_udfnum04') }},
        {{ trx_json_extract('src', ['POI_UDFNUM05'], type='float', alias='poi_udfnum05') }},
        {{ trx_json_extract('src', ['POI_UDFDATE01'], type='timestamp_ntz', alias='poi_udfdate01') }},
        {{ trx_json_extract('src', ['POI_UDFDATE02'], type='timestamp_ntz', alias='poi_udfdate02') }},
        {{ trx_json_extract('src', ['POI_UDFDATE03'], type='timestamp_ntz', alias='poi_udfdate03') }},
        {{ trx_json_extract('src', ['POI_UDFDATE04'], type='timestamp_ntz', alias='poi_udfdate04') }},
        {{ trx_json_extract('src', ['POI_UDFDATE05'], type='timestamp_ntz', alias='poi_udfdate05') }},
        {{ trx_json_extract('src', ['POI_UDFCHKBOX01'], type='varchar', alias='poi_udfchkbox01') }},
        {{ trx_json_extract('src', ['POI_UDFCHKBOX02'], type='varchar', alias='poi_udfchkbox02') }},
        {{ trx_json_extract('src', ['POI_UDFCHKBOX03'], type='varchar', alias='poi_udfchkbox03') }},
        {{ trx_json_extract('src', ['POI_UDFCHKBOX04'], type='varchar', alias='poi_udfchkbox04') }},
        {{ trx_json_extract('src', ['POI_UDFCHKBOX05'], type='varchar', alias='poi_udfchkbox05') }},
        {{ trx_json_extract('src', ['POI_OBJECT'], type='varchar', alias='poi_object') }},
        {{ trx_json_extract('src', ['POI_OBRTYPE'], type='varchar', alias='poi_obrtype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5points') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['poi_object', 'poi_object_org', 'poi_obtype', 'poi_point', 'poi_pointtype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['poi_lastsaved']) }}
