{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EVENTS_EAM1',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EVT_CODE'], type='varchar', alias='evt_code') }},
        {{ trx_json_extract('src', ['EVT_LASTSAVED'], type='date', alias='evt_lastsaved') }},
        {{ trx_json_extract('src', ['EVT_DESC'], type='varchar', alias='evt_desc') }},
        {{ trx_json_extract('src', ['EVT_PRIORITY'], type='varchar', alias='evt_priority') }},
        {{ trx_json_extract('src', ['EVT_MRC'], type='varchar', alias='evt_mrc') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5events_eam1') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_load_datetime']) }}
