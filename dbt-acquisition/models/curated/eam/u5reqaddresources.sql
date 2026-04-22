{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5REQADDRESOURCES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['RAR_EVENT'], type='varchar', alias='rar_event') }},
        {{ trx_json_extract('src', ['RAR_TYPE'], type='varchar', alias='rar_type') }},
        {{ trx_json_extract('src', ['RAR_NOTES'], type='varchar', alias='rar_notes') }},
        {{ trx_json_extract('src', ['RAR_HAZARDS'], type='varchar', alias='rar_hazards') }},
        {{ trx_json_extract('src', ['RAR_RESULTWONUM'], type='varchar', alias='rar_resultwonum') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['RAR_TRANSID'], type='varchar', alias='rar_transid') }},
        {{ trx_json_extract('src', ['RAR_RAISEDBY'], type='varchar', alias='rar_raisedby') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5reqaddresources') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rar_transid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
