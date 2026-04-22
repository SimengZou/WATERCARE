{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5OBJECTSERVICECODES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OSC_LASTSAVED'], type='timestamp_ntz', alias='osc_lastsaved') }},
        {{ trx_json_extract('src', ['OSC_OBJECT'], type='varchar', alias='osc_object') }},
        {{ trx_json_extract('src', ['OSC_OBJECT_ORG'], type='varchar', alias='osc_object_org') }},
        {{ trx_json_extract('src', ['OSC_SERVICECODE'], type='varchar', alias='osc_servicecode') }},
        {{ trx_json_extract('src', ['OSC_SERVICECODE_ORG'], type='varchar', alias='osc_servicecode_org') }},
        {{ trx_json_extract('src', ['OSC_APPLYTOCHILDREN'], type='varchar', alias='osc_applytochildren') }},
        {{ trx_json_extract('src', ['OSC_SKIPTHISLEVEL'], type='varchar', alias='osc_skipthislevel') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR01'], type='varchar', alias='osc_udfchar01') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR02'], type='varchar', alias='osc_udfchar02') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR03'], type='varchar', alias='osc_udfchar03') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR04'], type='varchar', alias='osc_udfchar04') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR05'], type='varchar', alias='osc_udfchar05') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR06'], type='varchar', alias='osc_udfchar06') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR07'], type='varchar', alias='osc_udfchar07') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR08'], type='varchar', alias='osc_udfchar08') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR09'], type='varchar', alias='osc_udfchar09') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR10'], type='varchar', alias='osc_udfchar10') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR11'], type='varchar', alias='osc_udfchar11') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR12'], type='varchar', alias='osc_udfchar12') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR13'], type='varchar', alias='osc_udfchar13') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR14'], type='varchar', alias='osc_udfchar14') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR15'], type='varchar', alias='osc_udfchar15') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR16'], type='varchar', alias='osc_udfchar16') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR17'], type='varchar', alias='osc_udfchar17') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR18'], type='varchar', alias='osc_udfchar18') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR19'], type='varchar', alias='osc_udfchar19') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR20'], type='varchar', alias='osc_udfchar20') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR21'], type='varchar', alias='osc_udfchar21') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR22'], type='varchar', alias='osc_udfchar22') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR23'], type='varchar', alias='osc_udfchar23') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR24'], type='varchar', alias='osc_udfchar24') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR25'], type='varchar', alias='osc_udfchar25') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR26'], type='varchar', alias='osc_udfchar26') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR27'], type='varchar', alias='osc_udfchar27') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR28'], type='varchar', alias='osc_udfchar28') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR29'], type='varchar', alias='osc_udfchar29') }},
        {{ trx_json_extract('src', ['OSC_UDFCHAR30'], type='varchar', alias='osc_udfchar30') }},
        {{ trx_json_extract('src', ['OSC_UDFNUM01'], type='float', alias='osc_udfnum01') }},
        {{ trx_json_extract('src', ['OSC_UDFNUM02'], type='float', alias='osc_udfnum02') }},
        {{ trx_json_extract('src', ['OSC_UDFNUM03'], type='float', alias='osc_udfnum03') }},
        {{ trx_json_extract('src', ['OSC_UDFNUM04'], type='float', alias='osc_udfnum04') }},
        {{ trx_json_extract('src', ['OSC_UDFNUM05'], type='float', alias='osc_udfnum05') }},
        {{ trx_json_extract('src', ['OSC_UDFDATE01'], type='timestamp_ntz', alias='osc_udfdate01') }},
        {{ trx_json_extract('src', ['OSC_UDFDATE02'], type='timestamp_ntz', alias='osc_udfdate02') }},
        {{ trx_json_extract('src', ['OSC_UDFDATE03'], type='timestamp_ntz', alias='osc_udfdate03') }},
        {{ trx_json_extract('src', ['OSC_UDFDATE04'], type='timestamp_ntz', alias='osc_udfdate04') }},
        {{ trx_json_extract('src', ['OSC_UDFDATE05'], type='timestamp_ntz', alias='osc_udfdate05') }},
        {{ trx_json_extract('src', ['OSC_UDFCHKBOX01'], type='varchar', alias='osc_udfchkbox01') }},
        {{ trx_json_extract('src', ['OSC_UDFCHKBOX02'], type='varchar', alias='osc_udfchkbox02') }},
        {{ trx_json_extract('src', ['OSC_UDFCHKBOX03'], type='varchar', alias='osc_udfchkbox03') }},
        {{ trx_json_extract('src', ['OSC_UDFCHKBOX04'], type='varchar', alias='osc_udfchkbox04') }},
        {{ trx_json_extract('src', ['OSC_UDFCHKBOX05'], type='varchar', alias='osc_udfchkbox05') }},
        {{ trx_json_extract('src', ['OSC_UPDATECOUNT'], type='float', alias='osc_updatecount') }},
        {{ trx_json_extract('src', ['OSC_PK'], type='varchar', alias='osc_pk') }},
        {{ trx_json_extract('src', ['OSC_PARENT'], type='varchar', alias='osc_parent') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5objectservicecodes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['osc_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['osc_lastsaved']) }}
