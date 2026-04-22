{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ORGANIZATION',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ORG_LASTSAVED'], type='timestamp_ntz', alias='org_lastsaved') }},
        {{ trx_json_extract('src', ['ORG_REQUESTRECALCDEP'], type='varchar', alias='org_requestrecalcdep') }},
        {{ trx_json_extract('src', ['ORG_CURR'], type='varchar', alias='org_curr') }},
        {{ trx_json_extract('src', ['ORG_COMMON'], type='varchar', alias='org_common') }},
        {{ trx_json_extract('src', ['ORG_CODEREF'], type='varchar', alias='org_coderef') }},
        {{ trx_json_extract('src', ['ORG_BOOKID'], type='varchar', alias='org_bookid') }},
        {{ trx_json_extract('src', ['ORG_MATCHLTA'], type='float', alias='org_matchlta') }},
        {{ trx_json_extract('src', ['ORG_MATCHLTP'], type='float', alias='org_matchltp') }},
        {{ trx_json_extract('src', ['ORG_TIMEZONE'], type='float', alias='org_timezone') }},
        {{ trx_json_extract('src', ['ORG_LATITUDE'], type='float', alias='org_latitude') }},
        {{ trx_json_extract('src', ['ORG_LONGITUDE'], type='float', alias='org_longitude') }},
        {{ trx_json_extract('src', ['ORG_EXTERNALORG'], type='varchar', alias='org_externalorg') }},
        {{ trx_json_extract('src', ['ORG_INVQTYTOL'], type='float', alias='org_invqtytol') }},
        {{ trx_json_extract('src', ['ORG_DUNSNUM'], type='varchar', alias='org_dunsnum') }},
        {{ trx_json_extract('src', ['ORG_UPDATECOUNT'], type='float', alias='org_updatecount') }},
        {{ trx_json_extract('src', ['ORG_CREATED'], type='timestamp_ntz', alias='org_created') }},
        {{ trx_json_extract('src', ['ORG_UPDATED'], type='timestamp_ntz', alias='org_updated') }},
        {{ trx_json_extract('src', ['ORG_LOCALE'], type='varchar', alias='org_locale') }},
        {{ trx_json_extract('src', ['ORG_DEPMETHOD'], type='varchar', alias='org_depmethod') }},
        {{ trx_json_extract('src', ['ORG_DEPRMETHOD'], type='varchar', alias='org_deprmethod') }},
        {{ trx_json_extract('src', ['ORG_SEGMENTVALUE'], type='varchar', alias='org_segmentvalue') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR01'], type='varchar', alias='org_udfchar01') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR02'], type='varchar', alias='org_udfchar02') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR03'], type='varchar', alias='org_udfchar03') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR04'], type='varchar', alias='org_udfchar04') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR05'], type='varchar', alias='org_udfchar05') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR06'], type='varchar', alias='org_udfchar06') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR07'], type='varchar', alias='org_udfchar07') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR08'], type='varchar', alias='org_udfchar08') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR09'], type='varchar', alias='org_udfchar09') }},
        {{ trx_json_extract('src', ['ORG_UDFCHAR10'], type='varchar', alias='org_udfchar10') }},
        {{ trx_json_extract('src', ['ORG_UDFNUM01'], type='float', alias='org_udfnum01') }},
        {{ trx_json_extract('src', ['ORG_UDFNUM02'], type='float', alias='org_udfnum02') }},
        {{ trx_json_extract('src', ['ORG_UDFNUM03'], type='float', alias='org_udfnum03') }},
        {{ trx_json_extract('src', ['ORG_UDFNUM04'], type='float', alias='org_udfnum04') }},
        {{ trx_json_extract('src', ['ORG_UDFNUM05'], type='float', alias='org_udfnum05') }},
        {{ trx_json_extract('src', ['ORG_UDFDATE01'], type='timestamp_ntz', alias='org_udfdate01') }},
        {{ trx_json_extract('src', ['ORG_UDFDATE02'], type='timestamp_ntz', alias='org_udfdate02') }},
        {{ trx_json_extract('src', ['ORG_UDFDATE03'], type='timestamp_ntz', alias='org_udfdate03') }},
        {{ trx_json_extract('src', ['ORG_UDFDATE04'], type='timestamp_ntz', alias='org_udfdate04') }},
        {{ trx_json_extract('src', ['ORG_UDFDATE05'], type='timestamp_ntz', alias='org_udfdate05') }},
        {{ trx_json_extract('src', ['ORG_UDFCHKBOX01'], type='varchar', alias='org_udfchkbox01') }},
        {{ trx_json_extract('src', ['ORG_UDFCHKBOX02'], type='varchar', alias='org_udfchkbox02') }},
        {{ trx_json_extract('src', ['ORG_UDFCHKBOX03'], type='varchar', alias='org_udfchkbox03') }},
        {{ trx_json_extract('src', ['ORG_UDFCHKBOX04'], type='varchar', alias='org_udfchkbox04') }},
        {{ trx_json_extract('src', ['ORG_UDFCHKBOX05'], type='varchar', alias='org_udfchkbox05') }},
        {{ trx_json_extract('src', ['ORG_ACCOUNTINGENTITY'], type='varchar', alias='org_accountingentity') }},
        {{ trx_json_extract('src', ['ORG_STREAMPLUSPROJECT'], type='varchar', alias='org_streamplusproject') }},
        {{ trx_json_extract('src', ['ORG_CALGROUPCODE'], type='varchar', alias='org_calgroupcode') }},
        {{ trx_json_extract('src', ['ORG_CALGROUPORG'], type='varchar', alias='org_calgrouporg') }},
        {{ trx_json_extract('src', ['ORG_CODE'], type='varchar', alias='org_code') }},
        {{ trx_json_extract('src', ['ORG_DESC'], type='varchar', alias='org_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5organization') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['org_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['org_lastsaved']) }}
