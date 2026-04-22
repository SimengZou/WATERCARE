{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SYSTEMWSPS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SWP_LASTSAVED'], type='timestamp_ntz', alias='swp_lastsaved') }},
        {{ trx_json_extract('src', ['SWP_TAB'], type='varchar', alias='swp_tab') }},
        {{ trx_json_extract('src', ['SWP_ACTIVE'], type='varchar', alias='swp_active') }},
        {{ trx_json_extract('src', ['SWP_WSPCODE'], type='varchar', alias='swp_wspcode') }},
        {{ trx_json_extract('src', ['SWP_FUNCTION'], type='varchar', alias='swp_function') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5systemwsps') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['swp_lastsaved']) }}
