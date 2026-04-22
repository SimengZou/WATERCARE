{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DRIVERCTRL',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DRV_LASTSAVED'], type='timestamp_ntz', alias='drv_lastsaved') }},
        {{ trx_json_extract('src', ['DRV_QUEUE'], type='varchar', alias='drv_queue') }},
        {{ trx_json_extract('src', ['DRV_ACTION'], type='varchar', alias='drv_action') }},
        {{ trx_json_extract('src', ['DRV_NEXTRUN'], type='timestamp_ntz', alias='drv_nextrun') }},
        {{ trx_json_extract('src', ['DRV_FREQUENCY'], type='float', alias='drv_frequency') }},
        {{ trx_json_extract('src', ['DRV_CODE'], type='varchar', alias='drv_code') }},
        {{ trx_json_extract('src', ['DRV_LASTRAN'], type='timestamp_ntz', alias='drv_lastran') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5driverctrl') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['drv_code', 'drv_queue']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['drv_lastsaved']) }}
