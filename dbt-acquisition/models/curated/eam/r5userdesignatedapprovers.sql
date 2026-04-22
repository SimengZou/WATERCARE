{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERDESIGNATEDAPPROVERS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UDA_LASTSAVED'], type='timestamp_ntz', alias='uda_lastsaved') }},
        {{ trx_json_extract('src', ['UDA_DESIGNATEDAPPROVER'], type='varchar', alias='uda_designatedapprover') }},
        {{ trx_json_extract('src', ['UDA_STATUS'], type='varchar', alias='uda_status') }},
        {{ trx_json_extract('src', ['UDA_RSTATUS'], type='varchar', alias='uda_rstatus') }},
        {{ trx_json_extract('src', ['UDA_STARTDATE'], type='timestamp_ntz', alias='uda_startdate') }},
        {{ trx_json_extract('src', ['UDA_ENDDATE'], type='timestamp_ntz', alias='uda_enddate') }},
        {{ trx_json_extract('src', ['UDA_REASON'], type='varchar', alias='uda_reason') }},
        {{ trx_json_extract('src', ['UDA_NOTES'], type='varchar', alias='uda_notes') }},
        {{ trx_json_extract('src', ['UDA_APPROVEDBY'], type='varchar', alias='uda_approvedby') }},
        {{ trx_json_extract('src', ['UDA_APPROVED'], type='timestamp_ntz', alias='uda_approved') }},
        {{ trx_json_extract('src', ['UDA_CREATEDBY'], type='varchar', alias='uda_createdby') }},
        {{ trx_json_extract('src', ['UDA_CREATED'], type='timestamp_ntz', alias='uda_created') }},
        {{ trx_json_extract('src', ['UDA_UPDATEDBY'], type='varchar', alias='uda_updatedby') }},
        {{ trx_json_extract('src', ['UDA_UPDATED'], type='timestamp_ntz', alias='uda_updated') }},
        {{ trx_json_extract('src', ['UDA_UDFCHAR01'], type='varchar', alias='uda_udfchar01') }},
        {{ trx_json_extract('src', ['UDA_UDFCHAR02'], type='varchar', alias='uda_udfchar02') }},
        {{ trx_json_extract('src', ['UDA_UDFCHAR03'], type='varchar', alias='uda_udfchar03') }},
        {{ trx_json_extract('src', ['UDA_UDFCHAR04'], type='varchar', alias='uda_udfchar04') }},
        {{ trx_json_extract('src', ['UDA_UDFCHAR05'], type='varchar', alias='uda_udfchar05') }},
        {{ trx_json_extract('src', ['UDA_UDFNUM01'], type='float', alias='uda_udfnum01') }},
        {{ trx_json_extract('src', ['UDA_UDFNUM02'], type='float', alias='uda_udfnum02') }},
        {{ trx_json_extract('src', ['UDA_UDFNUM03'], type='float', alias='uda_udfnum03') }},
        {{ trx_json_extract('src', ['UDA_UDFNUM04'], type='float', alias='uda_udfnum04') }},
        {{ trx_json_extract('src', ['UDA_UDFNUM05'], type='float', alias='uda_udfnum05') }},
        {{ trx_json_extract('src', ['UDA_UDFDATE01'], type='timestamp_ntz', alias='uda_udfdate01') }},
        {{ trx_json_extract('src', ['UDA_UDFDATE02'], type='timestamp_ntz', alias='uda_udfdate02') }},
        {{ trx_json_extract('src', ['UDA_UDFDATE03'], type='timestamp_ntz', alias='uda_udfdate03') }},
        {{ trx_json_extract('src', ['UDA_UDFDATE04'], type='timestamp_ntz', alias='uda_udfdate04') }},
        {{ trx_json_extract('src', ['UDA_UDFDATE05'], type='timestamp_ntz', alias='uda_udfdate05') }},
        {{ trx_json_extract('src', ['UDA_UDFCHKBOX01'], type='varchar', alias='uda_udfchkbox01') }},
        {{ trx_json_extract('src', ['UDA_UDFCHKBOX02'], type='varchar', alias='uda_udfchkbox02') }},
        {{ trx_json_extract('src', ['UDA_UDFCHKBOX03'], type='varchar', alias='uda_udfchkbox03') }},
        {{ trx_json_extract('src', ['UDA_UDFCHKBOX04'], type='varchar', alias='uda_udfchkbox04') }},
        {{ trx_json_extract('src', ['UDA_UDFCHKBOX05'], type='varchar', alias='uda_udfchkbox05') }},
        {{ trx_json_extract('src', ['UDA_UPDATECOUNT'], type='float', alias='uda_updatecount') }},
        {{ trx_json_extract('src', ['UDA_PK'], type='varchar', alias='uda_pk') }},
        {{ trx_json_extract('src', ['UDA_USER'], type='varchar', alias='uda_user') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userdesignatedapprovers') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['uda_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['uda_lastsaved']) }}
