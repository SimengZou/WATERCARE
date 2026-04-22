{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TRADES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TRD_LASTSAVED'], type='timestamp_ntz', alias='trd_lastsaved') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR27'], type='varchar', alias='trd_udfchar27') }},
        {{ trx_json_extract('src', ['TRD_UDFNUM01'], type='float', alias='trd_udfnum01') }},
        {{ trx_json_extract('src', ['TRD_UDFNUM02'], type='float', alias='trd_udfnum02') }},
        {{ trx_json_extract('src', ['TRD_UDFNUM03'], type='float', alias='trd_udfnum03') }},
        {{ trx_json_extract('src', ['TRD_UDFNUM04'], type='float', alias='trd_udfnum04') }},
        {{ trx_json_extract('src', ['TRD_UDFNUM05'], type='float', alias='trd_udfnum05') }},
        {{ trx_json_extract('src', ['TRD_UDFDATE01'], type='timestamp_ntz', alias='trd_udfdate01') }},
        {{ trx_json_extract('src', ['TRD_UDFDATE02'], type='timestamp_ntz', alias='trd_udfdate02') }},
        {{ trx_json_extract('src', ['TRD_UDFDATE03'], type='timestamp_ntz', alias='trd_udfdate03') }},
        {{ trx_json_extract('src', ['TRD_UDFDATE04'], type='timestamp_ntz', alias='trd_udfdate04') }},
        {{ trx_json_extract('src', ['TRD_UDFDATE05'], type='timestamp_ntz', alias='trd_udfdate05') }},
        {{ trx_json_extract('src', ['TRD_UDFCHKBOX01'], type='varchar', alias='trd_udfchkbox01') }},
        {{ trx_json_extract('src', ['TRD_UDFCHKBOX02'], type='varchar', alias='trd_udfchkbox02') }},
        {{ trx_json_extract('src', ['TRD_UDFCHKBOX03'], type='varchar', alias='trd_udfchkbox03') }},
        {{ trx_json_extract('src', ['TRD_UDFCHKBOX04'], type='varchar', alias='trd_udfchkbox04') }},
        {{ trx_json_extract('src', ['TRD_UDFCHKBOX05'], type='varchar', alias='trd_udfchkbox05') }},
        {{ trx_json_extract('src', ['TRD_AVAILABLEFORCUS'], type='varchar', alias='trd_availableforcus') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR28'], type='varchar', alias='trd_udfchar28') }},
        {{ trx_json_extract('src', ['TRD_CODE'], type='varchar', alias='trd_code') }},
        {{ trx_json_extract('src', ['TRD_DESC'], type='varchar', alias='trd_desc') }},
        {{ trx_json_extract('src', ['TRD_CLASS'], type='varchar', alias='trd_class') }},
        {{ trx_json_extract('src', ['TRD_ORG'], type='varchar', alias='trd_org') }},
        {{ trx_json_extract('src', ['TRD_CLASS_ORG'], type='varchar', alias='trd_class_org') }},
        {{ trx_json_extract('src', ['TRD_UPDATECOUNT'], type='float', alias='trd_updatecount') }},
        {{ trx_json_extract('src', ['TRD_CREATED'], type='timestamp_ntz', alias='trd_created') }},
        {{ trx_json_extract('src', ['TRD_UPDATED'], type='timestamp_ntz', alias='trd_updated') }},
        {{ trx_json_extract('src', ['TRD_NOTUSED'], type='varchar', alias='trd_notused') }},
        {{ trx_json_extract('src', ['TRD_LASTSTATUSUPDATE'], type='timestamp_ntz', alias='trd_laststatusupdate') }},
        {{ trx_json_extract('src', ['TRD_ABBREVIATION'], type='varchar', alias='trd_abbreviation') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR01'], type='varchar', alias='trd_udfchar01') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR02'], type='varchar', alias='trd_udfchar02') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR03'], type='varchar', alias='trd_udfchar03') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR04'], type='varchar', alias='trd_udfchar04') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR05'], type='varchar', alias='trd_udfchar05') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR06'], type='varchar', alias='trd_udfchar06') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR07'], type='varchar', alias='trd_udfchar07') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR08'], type='varchar', alias='trd_udfchar08') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR09'], type='varchar', alias='trd_udfchar09') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR10'], type='varchar', alias='trd_udfchar10') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR11'], type='varchar', alias='trd_udfchar11') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR12'], type='varchar', alias='trd_udfchar12') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR13'], type='varchar', alias='trd_udfchar13') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR14'], type='varchar', alias='trd_udfchar14') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR15'], type='varchar', alias='trd_udfchar15') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR16'], type='varchar', alias='trd_udfchar16') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR17'], type='varchar', alias='trd_udfchar17') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR18'], type='varchar', alias='trd_udfchar18') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR19'], type='varchar', alias='trd_udfchar19') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR20'], type='varchar', alias='trd_udfchar20') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR21'], type='varchar', alias='trd_udfchar21') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR22'], type='varchar', alias='trd_udfchar22') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR23'], type='varchar', alias='trd_udfchar23') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR24'], type='varchar', alias='trd_udfchar24') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR25'], type='varchar', alias='trd_udfchar25') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR26'], type='varchar', alias='trd_udfchar26') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR29'], type='varchar', alias='trd_udfchar29') }},
        {{ trx_json_extract('src', ['TRD_UDFCHAR30'], type='varchar', alias='trd_udfchar30') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5trades') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['trd_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['trd_lastsaved']) }}
