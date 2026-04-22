{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REMEMBER',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RMB_LASTSAVED'], type='timestamp_ntz', alias='rmb_lastsaved') }},
        {{ trx_json_extract('src', ['RMB_ITEM'], type='varchar', alias='rmb_item') }},
        {{ trx_json_extract('src', ['RMB_RENTITY'], type='varchar', alias='rmb_rentity') }},
        {{ trx_json_extract('src', ['RMB_UPDATECOUNT'], type='float', alias='rmb_updatecount') }},
        {{ trx_json_extract('src', ['RMB_FUNCTION'], type='varchar', alias='rmb_function') }},
        {{ trx_json_extract('src', ['RMB_ITEM2'], type='varchar', alias='rmb_item2') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5remember') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rmb_function', 'rmb_rentity', 'rmb_item']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rmb_lastsaved']) }}
