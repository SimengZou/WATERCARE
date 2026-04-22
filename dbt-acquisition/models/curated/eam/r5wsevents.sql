{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSEVENTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WSE_LASTSAVED'], type='timestamp_ntz', alias='wse_lastsaved') }},
        {{ trx_json_extract('src', ['WSE_DESC'], type='varchar', alias='wse_desc') }},
        {{ trx_json_extract('src', ['WSE_BASE_EVENT'], type='varchar', alias='wse_base_event') }},
        {{ trx_json_extract('src', ['WSE_MSG_TEMPLATE'], type='varchar', alias='wse_msg_template') }},
        {{ trx_json_extract('src', ['WSE_UPDATECOUNT'], type='float', alias='wse_updatecount') }},
        {{ trx_json_extract('src', ['WSE_MEKEY'], type='varchar', alias='wse_mekey') }},
        {{ trx_json_extract('src', ['WSE_CODE'], type='varchar', alias='wse_code') }},
        {{ trx_json_extract('src', ['WSE_FILTER_PROCESSOR'], type='varchar', alias='wse_filter_processor') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wsevents') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wse_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wse_lastsaved']) }}
