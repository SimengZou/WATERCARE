{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TRADERATES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TRR_LASTSAVED'], type='timestamp_ntz', alias='trr_lastsaved') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR16'], type='varchar', alias='trr_udfchar16') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR19'], type='varchar', alias='trr_udfchar19') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR20'], type='varchar', alias='trr_udfchar20') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR21'], type='varchar', alias='trr_udfchar21') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR22'], type='varchar', alias='trr_udfchar22') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR23'], type='varchar', alias='trr_udfchar23') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR24'], type='varchar', alias='trr_udfchar24') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR25'], type='varchar', alias='trr_udfchar25') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR26'], type='varchar', alias='trr_udfchar26') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR27'], type='varchar', alias='trr_udfchar27') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR28'], type='varchar', alias='trr_udfchar28') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR29'], type='varchar', alias='trr_udfchar29') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR30'], type='varchar', alias='trr_udfchar30') }},
        {{ trx_json_extract('src', ['TRR_UDFNUM01'], type='float', alias='trr_udfnum01') }},
        {{ trx_json_extract('src', ['TRR_UDFNUM02'], type='float', alias='trr_udfnum02') }},
        {{ trx_json_extract('src', ['TRR_UDFNUM03'], type='float', alias='trr_udfnum03') }},
        {{ trx_json_extract('src', ['TRR_UDFNUM04'], type='float', alias='trr_udfnum04') }},
        {{ trx_json_extract('src', ['TRR_UDFNUM05'], type='float', alias='trr_udfnum05') }},
        {{ trx_json_extract('src', ['TRR_UDFDATE01'], type='timestamp_ntz', alias='trr_udfdate01') }},
        {{ trx_json_extract('src', ['TRR_UDFDATE02'], type='timestamp_ntz', alias='trr_udfdate02') }},
        {{ trx_json_extract('src', ['TRR_UDFDATE03'], type='timestamp_ntz', alias='trr_udfdate03') }},
        {{ trx_json_extract('src', ['TRR_UDFDATE04'], type='timestamp_ntz', alias='trr_udfdate04') }},
        {{ trx_json_extract('src', ['TRR_UDFDATE05'], type='timestamp_ntz', alias='trr_udfdate05') }},
        {{ trx_json_extract('src', ['TRR_UDFCHKBOX01'], type='varchar', alias='trr_udfchkbox01') }},
        {{ trx_json_extract('src', ['TRR_UDFCHKBOX02'], type='varchar', alias='trr_udfchkbox02') }},
        {{ trx_json_extract('src', ['TRR_UDFCHKBOX03'], type='varchar', alias='trr_udfchkbox03') }},
        {{ trx_json_extract('src', ['TRR_UDFCHKBOX04'], type='varchar', alias='trr_udfchkbox04') }},
        {{ trx_json_extract('src', ['TRR_UDFCHKBOX05'], type='varchar', alias='trr_udfchkbox05') }},
        {{ trx_json_extract('src', ['TRR_CUSTANDARDRATE'], type='float', alias='trr_custandardrate') }},
        {{ trx_json_extract('src', ['TRR_MRC'], type='varchar', alias='trr_mrc') }},
        {{ trx_json_extract('src', ['TRR_TRADE'], type='varchar', alias='trr_trade') }},
        {{ trx_json_extract('src', ['TRR_SUPPLIER'], type='varchar', alias='trr_supplier') }},
        {{ trx_json_extract('src', ['TRR_START'], type='timestamp_ntz', alias='trr_start') }},
        {{ trx_json_extract('src', ['TRR_END'], type='timestamp_ntz', alias='trr_end') }},
        {{ trx_json_extract('src', ['TRR_NTRATE'], type='float', alias='trr_ntrate') }},
        {{ trx_json_extract('src', ['TRR_OTRATE'], type='float', alias='trr_otrate') }},
        {{ trx_json_extract('src', ['TRR_ACTIVE'], type='varchar', alias='trr_active') }},
        {{ trx_json_extract('src', ['TRR_OCTYPE'], type='varchar', alias='trr_octype') }},
        {{ trx_json_extract('src', ['TRR_SUPPLIER_ORG'], type='varchar', alias='trr_supplier_org') }},
        {{ trx_json_extract('src', ['TRR_ORG'], type='varchar', alias='trr_org') }},
        {{ trx_json_extract('src', ['TRR_UPDATECOUNT'], type='float', alias='trr_updatecount') }},
        {{ trx_json_extract('src', ['TRR_PERSON'], type='varchar', alias='trr_person') }},
        {{ trx_json_extract('src', ['TRR_TAX'], type='varchar', alias='trr_tax') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR01'], type='varchar', alias='trr_udfchar01') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR02'], type='varchar', alias='trr_udfchar02') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR03'], type='varchar', alias='trr_udfchar03') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR04'], type='varchar', alias='trr_udfchar04') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR05'], type='varchar', alias='trr_udfchar05') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR06'], type='varchar', alias='trr_udfchar06') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR07'], type='varchar', alias='trr_udfchar07') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR08'], type='varchar', alias='trr_udfchar08') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR09'], type='varchar', alias='trr_udfchar09') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR10'], type='varchar', alias='trr_udfchar10') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR11'], type='varchar', alias='trr_udfchar11') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR12'], type='varchar', alias='trr_udfchar12') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR13'], type='varchar', alias='trr_udfchar13') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR14'], type='varchar', alias='trr_udfchar14') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR15'], type='varchar', alias='trr_udfchar15') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR17'], type='varchar', alias='trr_udfchar17') }},
        {{ trx_json_extract('src', ['TRR_UDFCHAR18'], type='varchar', alias='trr_udfchar18') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5traderates') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['trr_mrc', 'trr_trade', 'trr_supplier', 'trr_supplier_org', 'trr_person', 'trr_org', 'trr_start', 'trr_octype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['trr_lastsaved']) }}
