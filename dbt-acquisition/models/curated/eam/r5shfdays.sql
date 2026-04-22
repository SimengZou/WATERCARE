{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SHFDAYS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SHD_LASTSAVED'], type='timestamp_ntz', alias='shd_lastsaved') }},
        {{ trx_json_extract('src', ['SHD_DAY'], type='float', alias='shd_day') }},
        {{ trx_json_extract('src', ['SHD_HOURS'], type='float', alias='shd_hours') }},
        {{ trx_json_extract('src', ['SHD_UPDATECOUNT'], type='float', alias='shd_updatecount') }},
        {{ trx_json_extract('src', ['SHD_SHIFT'], type='varchar', alias='shd_shift') }},
        {{ trx_json_extract('src', ['SHD_TIME'], type='float', alias='shd_time') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5shfdays') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['shd_shift', 'shd_day', 'shd_time']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['shd_lastsaved']) }}
