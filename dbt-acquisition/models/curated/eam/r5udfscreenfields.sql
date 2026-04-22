{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5UDFSCREENFIELDS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['USF_LASTSAVED'], type='timestamp_ntz', alias='usf_lastsaved') }},
        {{ trx_json_extract('src', ['USF_DESC'], type='varchar', alias='usf_desc') }},
        {{ trx_json_extract('src', ['USF_SEQUENCE'], type='float', alias='usf_sequence') }},
        {{ trx_json_extract('src', ['USF_MAXLENGTH'], type='float', alias='usf_maxlength') }},
        {{ trx_json_extract('src', ['USF_PRECISION'], type='float', alias='usf_precision') }},
        {{ trx_json_extract('src', ['USF_SCALE'], type='float', alias='usf_scale') }},
        {{ trx_json_extract('src', ['USF_FIELDLABEL'], type='varchar', alias='usf_fieldlabel') }},
        {{ trx_json_extract('src', ['USF_FIELDTYPE'], type='varchar', alias='usf_fieldtype') }},
        {{ trx_json_extract('src', ['USF_PRIMARY'], type='varchar', alias='usf_primary') }},
        {{ trx_json_extract('src', ['USF_NOTUSED'], type='varchar', alias='usf_notused') }},
        {{ trx_json_extract('src', ['USF_NULLABLE'], type='varchar', alias='usf_nullable') }},
        {{ trx_json_extract('src', ['USF_PARENTFIELD'], type='varchar', alias='usf_parentfield') }},
        {{ trx_json_extract('src', ['USF_LOOKUPSOURCE'], type='varchar', alias='usf_lookupsource') }},
        {{ trx_json_extract('src', ['USF_LOOKUPQUERY'], type='varchar', alias='usf_lookupquery') }},
        {{ trx_json_extract('src', ['USF_COMPUTEDDATA'], type='varchar', alias='usf_computeddata') }},
        {{ trx_json_extract('src', ['USF_UPPERCASE'], type='varchar', alias='usf_uppercase') }},
        {{ trx_json_extract('src', ['USF_CREATED'], type='timestamp_ntz', alias='usf_created') }},
        {{ trx_json_extract('src', ['USF_UPDATED'], type='timestamp_ntz', alias='usf_updated') }},
        {{ trx_json_extract('src', ['USF_CREATEDBY'], type='varchar', alias='usf_createdby') }},
        {{ trx_json_extract('src', ['USF_UPDATEDBY'], type='varchar', alias='usf_updatedby') }},
        {{ trx_json_extract('src', ['USF_UPDATECOUNT'], type='float', alias='usf_updatecount') }},
        {{ trx_json_extract('src', ['USF_GENERATED'], type='varchar', alias='usf_generated') }},
        {{ trx_json_extract('src', ['USF_RETRIEVEDVALUELOOKUP'], type='varchar', alias='usf_retrievedvaluelookup') }},
        {{ trx_json_extract('src', ['USF_DROPPDOWN'], type='varchar', alias='usf_droppdown') }},
        {{ trx_json_extract('src', ['USF_SCREENNAME'], type='varchar', alias='usf_screenname') }},
        {{ trx_json_extract('src', ['USF_FIELDNAME'], type='varchar', alias='usf_fieldname') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5udfscreenfields') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['usf_screenname', 'usf_fieldname']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['usf_lastsaved']) }}
