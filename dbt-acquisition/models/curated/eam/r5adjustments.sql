{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ADJUSTMENTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ADJ_LASTSAVED'], type='timestamp_ntz', alias='adj_lastsaved') }},
        {{ trx_json_extract('src', ['ADJ_CODE'], type='varchar', alias='adj_code') }},
        {{ trx_json_extract('src', ['ADJ_RATE'], type='float', alias='adj_rate') }},
        {{ trx_json_extract('src', ['ADJ_STWO'], type='varchar', alias='adj_stwo') }},
        {{ trx_json_extract('src', ['ADJ_NOTUSED'], type='varchar', alias='adj_notused') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR01'], type='varchar', alias='adj_udfchar01') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR02'], type='varchar', alias='adj_udfchar02') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR03'], type='varchar', alias='adj_udfchar03') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR04'], type='varchar', alias='adj_udfchar04') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR05'], type='varchar', alias='adj_udfchar05') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR06'], type='varchar', alias='adj_udfchar06') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR07'], type='varchar', alias='adj_udfchar07') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR08'], type='varchar', alias='adj_udfchar08') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR09'], type='varchar', alias='adj_udfchar09') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR10'], type='varchar', alias='adj_udfchar10') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR11'], type='varchar', alias='adj_udfchar11') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR12'], type='varchar', alias='adj_udfchar12') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR13'], type='varchar', alias='adj_udfchar13') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR14'], type='varchar', alias='adj_udfchar14') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR15'], type='varchar', alias='adj_udfchar15') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR16'], type='varchar', alias='adj_udfchar16') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR17'], type='varchar', alias='adj_udfchar17') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR18'], type='varchar', alias='adj_udfchar18') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR19'], type='varchar', alias='adj_udfchar19') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR20'], type='varchar', alias='adj_udfchar20') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR21'], type='varchar', alias='adj_udfchar21') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR22'], type='varchar', alias='adj_udfchar22') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR23'], type='varchar', alias='adj_udfchar23') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR24'], type='varchar', alias='adj_udfchar24') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR25'], type='varchar', alias='adj_udfchar25') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR26'], type='varchar', alias='adj_udfchar26') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR27'], type='varchar', alias='adj_udfchar27') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR28'], type='varchar', alias='adj_udfchar28') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR29'], type='varchar', alias='adj_udfchar29') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHAR30'], type='varchar', alias='adj_udfchar30') }},
        {{ trx_json_extract('src', ['ADJ_UDFNUM01'], type='float', alias='adj_udfnum01') }},
        {{ trx_json_extract('src', ['ADJ_UDFNUM02'], type='float', alias='adj_udfnum02') }},
        {{ trx_json_extract('src', ['ADJ_UDFNUM03'], type='float', alias='adj_udfnum03') }},
        {{ trx_json_extract('src', ['ADJ_UDFNUM04'], type='float', alias='adj_udfnum04') }},
        {{ trx_json_extract('src', ['ADJ_UDFNUM05'], type='float', alias='adj_udfnum05') }},
        {{ trx_json_extract('src', ['ADJ_UDFDATE01'], type='timestamp_ntz', alias='adj_udfdate01') }},
        {{ trx_json_extract('src', ['ADJ_UDFDATE02'], type='timestamp_ntz', alias='adj_udfdate02') }},
        {{ trx_json_extract('src', ['ADJ_UDFDATE03'], type='timestamp_ntz', alias='adj_udfdate03') }},
        {{ trx_json_extract('src', ['ADJ_UDFDATE04'], type='timestamp_ntz', alias='adj_udfdate04') }},
        {{ trx_json_extract('src', ['ADJ_UDFDATE05'], type='timestamp_ntz', alias='adj_udfdate05') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHKBOX01'], type='varchar', alias='adj_udfchkbox01') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHKBOX02'], type='varchar', alias='adj_udfchkbox02') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHKBOX03'], type='varchar', alias='adj_udfchkbox03') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHKBOX04'], type='varchar', alias='adj_udfchkbox04') }},
        {{ trx_json_extract('src', ['ADJ_UDFCHKBOX05'], type='varchar', alias='adj_udfchkbox05') }},
        {{ trx_json_extract('src', ['ADJ_UPDATECOUNT'], type='float', alias='adj_updatecount') }},
        {{ trx_json_extract('src', ['ADJ_ORG'], type='varchar', alias='adj_org') }},
        {{ trx_json_extract('src', ['ADJ_DESC'], type='varchar', alias='adj_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5adjustments') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['adj_code', 'adj_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['adj_lastsaved']) }}
