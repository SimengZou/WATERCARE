{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5UDFSCREENS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['USC_LASTSAVED'], type='timestamp_ntz', alias='usc_lastsaved') }},
        {{ trx_json_extract('src', ['USC_TABLENAME'], type='varchar', alias='usc_tablename') }},
        {{ trx_json_extract('src', ['USC_ISTAB'], type='varchar', alias='usc_istab') }},
        {{ trx_json_extract('src', ['USC_PARENTSCREEN'], type='varchar', alias='usc_parentscreen') }},
        {{ trx_json_extract('src', ['USC_GENERATED'], type='varchar', alias='usc_generated') }},
        {{ trx_json_extract('src', ['USC_NOTUSED'], type='varchar', alias='usc_notused') }},
        {{ trx_json_extract('src', ['USC_CREATED'], type='timestamp_ntz', alias='usc_created') }},
        {{ trx_json_extract('src', ['USC_UPDATED'], type='timestamp_ntz', alias='usc_updated') }},
        {{ trx_json_extract('src', ['USC_CREATEDBY'], type='varchar', alias='usc_createdby') }},
        {{ trx_json_extract('src', ['USC_UPDATEDBY'], type='varchar', alias='usc_updatedby') }},
        {{ trx_json_extract('src', ['USC_UPDATECOUNT'], type='float', alias='usc_updatecount') }},
        {{ trx_json_extract('src', ['USC_ENTITY'], type='varchar', alias='usc_entity') }},
        {{ trx_json_extract('src', ['USC_ALLOWCOMMENTS'], type='varchar', alias='usc_allowcomments') }},
        {{ trx_json_extract('src', ['USC_ALLOWDOCUMENTS'], type='varchar', alias='usc_allowdocuments') }},
        {{ trx_json_extract('src', ['USC_TYPEENTITY'], type='varchar', alias='usc_typeentity') }},
        {{ trx_json_extract('src', ['USC_STATUSENTITY'], type='varchar', alias='usc_statusentity') }},
        {{ trx_json_extract('src', ['USC_CLASS'], type='varchar', alias='usc_class') }},
        {{ trx_json_extract('src', ['USC_ORGSECURITY'], type='varchar', alias='usc_orgsecurity') }},
        {{ trx_json_extract('src', ['USC_AUTOGENERATEKEY'], type='varchar', alias='usc_autogeneratekey') }},
        {{ trx_json_extract('src', ['USC_SCREENNAME'], type='varchar', alias='usc_screenname') }},
        {{ trx_json_extract('src', ['USC_DESC'], type='varchar', alias='usc_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5udfscreens') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['usc_screenname']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['usc_lastsaved']) }}
