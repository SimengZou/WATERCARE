{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5PMPRIORTIES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['PMP_CYCLE_UOM'], type='varchar', alias='pmp_cycle_uom') }},
        {{ trx_json_extract('src', ['PMP_CYCLE_TEXT'], type='varchar', alias='pmp_cycle_text') }},
        {{ trx_json_extract('src', ['PMP_RELEASE_DAYS'], type='float', alias='pmp_release_days') }},
        {{ trx_json_extract('src', ['PMP_PRIORITY'], type='varchar', alias='pmp_priority') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['PMP_CYCLE_LENGTH'], type='float', alias='pmp_cycle_length') }},
        {{ trx_json_extract('src', ['PMP_COMPLETE_DAYS'], type='float', alias='pmp_complete_days') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5pmpriorties') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pmp_cycle_length', 'pmp_cycle_uom']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
