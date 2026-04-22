{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TENANTSTATUS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TPS_LASTSAVED'], type='timestamp_ntz', alias='tps_lastsaved') }},
        {{ trx_json_extract('src', ['TPS_UPDATECOUNT'], type='float', alias='tps_updatecount') }},
        {{ trx_json_extract('src', ['TPS_STATUS'], type='varchar', alias='tps_status') }},
        {{ trx_json_extract('src', ['TPS_UPDATETIMESTAMP'], type='timestamp_ntz', alias='tps_updatetimestamp') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5tenantstatus') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['tps_lastsaved']) }}
