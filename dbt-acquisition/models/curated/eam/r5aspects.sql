{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ASPECTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ASP_LASTSAVED'], type='timestamp_ntz', alias='asp_lastsaved') }},
        {{ trx_json_extract('src', ['ASP_CLASS'], type='varchar', alias='asp_class') }},
        {{ trx_json_extract('src', ['ASP_TIMEDEP'], type='varchar', alias='asp_timedep') }},
        {{ trx_json_extract('src', ['ASP_CLASS_ORG'], type='varchar', alias='asp_class_org') }},
        {{ trx_json_extract('src', ['ASP_PARENT'], type='varchar', alias='asp_parent') }},
        {{ trx_json_extract('src', ['ASP_NOTUSED'], type='varchar', alias='asp_notused') }},
        {{ trx_json_extract('src', ['ASP_RANDOM'], type='varchar', alias='asp_random') }},
        {{ trx_json_extract('src', ['ASP_UPDATECOUNT'], type='float', alias='asp_updatecount') }},
        {{ trx_json_extract('src', ['ASP_CREATED'], type='timestamp_ntz', alias='asp_created') }},
        {{ trx_json_extract('src', ['ASP_UPDATED'], type='timestamp_ntz', alias='asp_updated') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR01'], type='varchar', alias='asp_udfchar01') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR02'], type='varchar', alias='asp_udfchar02') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR03'], type='varchar', alias='asp_udfchar03') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR04'], type='varchar', alias='asp_udfchar04') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR05'], type='varchar', alias='asp_udfchar05') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR06'], type='varchar', alias='asp_udfchar06') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR07'], type='varchar', alias='asp_udfchar07') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR08'], type='varchar', alias='asp_udfchar08') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR09'], type='varchar', alias='asp_udfchar09') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR10'], type='varchar', alias='asp_udfchar10') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR11'], type='varchar', alias='asp_udfchar11') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR12'], type='varchar', alias='asp_udfchar12') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR13'], type='varchar', alias='asp_udfchar13') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR14'], type='varchar', alias='asp_udfchar14') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR15'], type='varchar', alias='asp_udfchar15') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR16'], type='varchar', alias='asp_udfchar16') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR17'], type='varchar', alias='asp_udfchar17') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR18'], type='varchar', alias='asp_udfchar18') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR19'], type='varchar', alias='asp_udfchar19') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR20'], type='varchar', alias='asp_udfchar20') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR21'], type='varchar', alias='asp_udfchar21') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR22'], type='varchar', alias='asp_udfchar22') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR23'], type='varchar', alias='asp_udfchar23') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR24'], type='varchar', alias='asp_udfchar24') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR25'], type='varchar', alias='asp_udfchar25') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR26'], type='varchar', alias='asp_udfchar26') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR27'], type='varchar', alias='asp_udfchar27') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR28'], type='varchar', alias='asp_udfchar28') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR29'], type='varchar', alias='asp_udfchar29') }},
        {{ trx_json_extract('src', ['ASP_UDFCHAR30'], type='varchar', alias='asp_udfchar30') }},
        {{ trx_json_extract('src', ['ASP_UDFNUM01'], type='float', alias='asp_udfnum01') }},
        {{ trx_json_extract('src', ['ASP_UDFNUM02'], type='float', alias='asp_udfnum02') }},
        {{ trx_json_extract('src', ['ASP_UDFNUM03'], type='float', alias='asp_udfnum03') }},
        {{ trx_json_extract('src', ['ASP_UDFNUM04'], type='float', alias='asp_udfnum04') }},
        {{ trx_json_extract('src', ['ASP_UDFNUM05'], type='float', alias='asp_udfnum05') }},
        {{ trx_json_extract('src', ['ASP_UDFDATE01'], type='timestamp_ntz', alias='asp_udfdate01') }},
        {{ trx_json_extract('src', ['ASP_UDFDATE02'], type='timestamp_ntz', alias='asp_udfdate02') }},
        {{ trx_json_extract('src', ['ASP_UDFDATE03'], type='timestamp_ntz', alias='asp_udfdate03') }},
        {{ trx_json_extract('src', ['ASP_UDFDATE04'], type='timestamp_ntz', alias='asp_udfdate04') }},
        {{ trx_json_extract('src', ['ASP_UDFDATE05'], type='timestamp_ntz', alias='asp_udfdate05') }},
        {{ trx_json_extract('src', ['ASP_UDFCHKBOX01'], type='varchar', alias='asp_udfchkbox01') }},
        {{ trx_json_extract('src', ['ASP_UDFCHKBOX02'], type='varchar', alias='asp_udfchkbox02') }},
        {{ trx_json_extract('src', ['ASP_UDFCHKBOX03'], type='varchar', alias='asp_udfchkbox03') }},
        {{ trx_json_extract('src', ['ASP_UDFCHKBOX04'], type='varchar', alias='asp_udfchkbox04') }},
        {{ trx_json_extract('src', ['ASP_UDFCHKBOX05'], type='varchar', alias='asp_udfchkbox05') }},
        {{ trx_json_extract('src', ['ASP_CODE'], type='varchar', alias='asp_code') }},
        {{ trx_json_extract('src', ['ASP_DESC'], type='varchar', alias='asp_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5aspects') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['asp_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['asp_lastsaved']) }}
