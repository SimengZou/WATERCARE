{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ALT_LASTSAVED'], type='timestamp_ntz', alias='alt_lastsaved') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR01'], type='varchar', alias='alt_udfchar01') }},
        {{ trx_json_extract('src', ['ALT_GRIDID'], type='float', alias='alt_gridid') }},
        {{ trx_json_extract('src', ['ALT_DATASPYID'], type='float', alias='alt_dataspyid') }},
        {{ trx_json_extract('src', ['ALT_FREQ'], type='float', alias='alt_freq') }},
        {{ trx_json_extract('src', ['ALT_FREQUOM'], type='varchar', alias='alt_frequom') }},
        {{ trx_json_extract('src', ['ALT_NEXTEVAL'], type='timestamp_ntz', alias='alt_nexteval') }},
        {{ trx_json_extract('src', ['ALT_LASTEVAL'], type='timestamp_ntz', alias='alt_lasteval') }},
        {{ trx_json_extract('src', ['ALT_LASTALERT'], type='timestamp_ntz', alias='alt_lastalert') }},
        {{ trx_json_extract('src', ['ALT_USEMINMAX'], type='varchar', alias='alt_useminmax') }},
        {{ trx_json_extract('src', ['ALT_MINVALUE'], type='float', alias='alt_minvalue') }},
        {{ trx_json_extract('src', ['ALT_MAXVALUE'], type='float', alias='alt_maxvalue') }},
        {{ trx_json_extract('src', ['ALT_WITHINVALUES'], type='varchar', alias='alt_withinvalues') }},
        {{ trx_json_extract('src', ['ALT_VALUEFIELDID'], type='float', alias='alt_valuefieldid') }},
        {{ trx_json_extract('src', ['ALT_RENTITY'], type='varchar', alias='alt_rentity') }},
        {{ trx_json_extract('src', ['ALT_ENTITY'], type='varchar', alias='alt_entity') }},
        {{ trx_json_extract('src', ['ALT_CODEFIELDID'], type='float', alias='alt_codefieldid') }},
        {{ trx_json_extract('src', ['ALT_ORGFIELDID'], type='float', alias='alt_orgfieldid') }},
        {{ trx_json_extract('src', ['ALT_ACTIVE'], type='varchar', alias='alt_active') }},
        {{ trx_json_extract('src', ['ALT_ENABLEWO'], type='varchar', alias='alt_enablewo') }},
        {{ trx_json_extract('src', ['ALT_ENABLEMAIL'], type='varchar', alias='alt_enablemail') }},
        {{ trx_json_extract('src', ['ALT_INPROGRESS'], type='varchar', alias='alt_inprogress') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR02'], type='varchar', alias='alt_udfchar02') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR03'], type='varchar', alias='alt_udfchar03') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR04'], type='varchar', alias='alt_udfchar04') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR05'], type='varchar', alias='alt_udfchar05') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR06'], type='varchar', alias='alt_udfchar06') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR07'], type='varchar', alias='alt_udfchar07') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR08'], type='varchar', alias='alt_udfchar08') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR09'], type='varchar', alias='alt_udfchar09') }},
        {{ trx_json_extract('src', ['ALT_UDFCHAR10'], type='varchar', alias='alt_udfchar10') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM01'], type='float', alias='alt_udfnum01') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM02'], type='float', alias='alt_udfnum02') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM03'], type='float', alias='alt_udfnum03') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM04'], type='float', alias='alt_udfnum04') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM05'], type='float', alias='alt_udfnum05') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM06'], type='float', alias='alt_udfnum06') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM07'], type='float', alias='alt_udfnum07') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM08'], type='float', alias='alt_udfnum08') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM09'], type='float', alias='alt_udfnum09') }},
        {{ trx_json_extract('src', ['ALT_UDFNUM10'], type='float', alias='alt_udfnum10') }},
        {{ trx_json_extract('src', ['ALT_UDFDATE01'], type='timestamp_ntz', alias='alt_udfdate01') }},
        {{ trx_json_extract('src', ['ALT_UDFDATE02'], type='timestamp_ntz', alias='alt_udfdate02') }},
        {{ trx_json_extract('src', ['ALT_UDFDATE03'], type='timestamp_ntz', alias='alt_udfdate03') }},
        {{ trx_json_extract('src', ['ALT_UDFDATE04'], type='timestamp_ntz', alias='alt_udfdate04') }},
        {{ trx_json_extract('src', ['ALT_UDFDATE05'], type='timestamp_ntz', alias='alt_udfdate05') }},
        {{ trx_json_extract('src', ['ALT_UDFCHKBOX01'], type='varchar', alias='alt_udfchkbox01') }},
        {{ trx_json_extract('src', ['ALT_UDFCHKBOX02'], type='varchar', alias='alt_udfchkbox02') }},
        {{ trx_json_extract('src', ['ALT_UDFCHKBOX03'], type='varchar', alias='alt_udfchkbox03') }},
        {{ trx_json_extract('src', ['ALT_UDFCHKBOX04'], type='varchar', alias='alt_udfchkbox04') }},
        {{ trx_json_extract('src', ['ALT_UDFCHKBOX05'], type='varchar', alias='alt_udfchkbox05') }},
        {{ trx_json_extract('src', ['ALT_CREATED'], type='timestamp_ntz', alias='alt_created') }},
        {{ trx_json_extract('src', ['ALT_CREATEDBY'], type='varchar', alias='alt_createdby') }},
        {{ trx_json_extract('src', ['ALT_UPDATECOUNT'], type='float', alias='alt_updatecount') }},
        {{ trx_json_extract('src', ['ALT_SOURCE'], type='varchar', alias='alt_source') }},
        {{ trx_json_extract('src', ['ALT_ENABLEIONPULSE'], type='varchar', alias='alt_enableionpulse') }},
        {{ trx_json_extract('src', ['ALT_ENABLEGENERATEWO'], type='varchar', alias='alt_enablegeneratewo') }},
        {{ trx_json_extract('src', ['ALT_IMPORT_REF'], type='varchar', alias='alt_import_ref') }},
        {{ trx_json_extract('src', ['ALT_ENABLEWOPICKTICKET'], type='varchar', alias='alt_enablewopickticket') }},
        {{ trx_json_extract('src', ['ALT_CODE'], type='varchar', alias='alt_code') }},
        {{ trx_json_extract('src', ['ALT_DESC'], type='varchar', alias='alt_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alerts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['alt_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['alt_lastsaved']) }}
