{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WARCOVERAGES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WCV_LASTSAVED'], type='timestamp_ntz', alias='wcv_lastsaved') }},
        {{ trx_json_extract('src', ['WCV_OBTYPE'], type='varchar', alias='wcv_obtype') }},
        {{ trx_json_extract('src', ['WCV_OBRTYPE'], type='varchar', alias='wcv_obrtype') }},
        {{ trx_json_extract('src', ['WCV_UOM'], type='varchar', alias='wcv_uom') }},
        {{ trx_json_extract('src', ['WCV_DURATION'], type='float', alias='wcv_duration') }},
        {{ trx_json_extract('src', ['WCV_EXPIRATION'], type='float', alias='wcv_expiration') }},
        {{ trx_json_extract('src', ['WCV_EXPIRATIONDATE'], type='timestamp_ntz', alias='wcv_expirationdate') }},
        {{ trx_json_extract('src', ['WCV_NEARTHRESHOLD'], type='float', alias='wcv_nearthreshold') }},
        {{ trx_json_extract('src', ['WCV_STARTUSAGE'], type='float', alias='wcv_startusage') }},
        {{ trx_json_extract('src', ['WCV_SEQNO'], type='float', alias='wcv_seqno') }},
        {{ trx_json_extract('src', ['WCV_ACTIVE'], type='varchar', alias='wcv_active') }},
        {{ trx_json_extract('src', ['WCV_STARTDATE'], type='timestamp_ntz', alias='wcv_startdate') }},
        {{ trx_json_extract('src', ['WCV_RECORDED'], type='timestamp_ntz', alias='wcv_recorded') }},
        {{ trx_json_extract('src', ['WCV_USER'], type='varchar', alias='wcv_user') }},
        {{ trx_json_extract('src', ['WCV_OBJECT_ORG'], type='varchar', alias='wcv_object_org') }},
        {{ trx_json_extract('src', ['WCV_UPDATECOUNT'], type='float', alias='wcv_updatecount') }},
        {{ trx_json_extract('src', ['WCV_CREATED'], type='timestamp_ntz', alias='wcv_created') }},
        {{ trx_json_extract('src', ['WCV_UPDATED'], type='timestamp_ntz', alias='wcv_updated') }},
        {{ trx_json_extract('src', ['WCV_WARRANTY'], type='varchar', alias='wcv_warranty') }},
        {{ trx_json_extract('src', ['WCV_OBJECT'], type='varchar', alias='wcv_object') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5warcoverages') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wcv_seqno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wcv_lastsaved']) }}
