{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSRSPHIST',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WSR_LASTSAVED'], type='timestamp_ntz', alias='wsr_lastsaved') }},
        {{ trx_json_extract('src', ['WSR_ADDRESS'], type='varchar', alias='wsr_address') }},
        {{ trx_json_extract('src', ['WSR_MESSAGE'], type='varchar', alias='wsr_message') }},
        {{ trx_json_extract('src', ['WSR_DATAAREA'], type='float', alias='wsr_dataarea') }},
        {{ trx_json_extract('src', ['WSR_TIME'], type='timestamp_ntz', alias='wsr_time') }},
        {{ trx_json_extract('src', ['WSR_STATUS'], type='varchar', alias='wsr_status') }},
        {{ trx_json_extract('src', ['WSR_RSTATUS'], type='varchar', alias='wsr_rstatus') }},
        {{ trx_json_extract('src', ['WSR_STATUS_MESSAGE'], type='varchar', alias='wsr_status_message') }},
        {{ trx_json_extract('src', ['WSR_UPDATECOUNT'], type='float', alias='wsr_updatecount') }},
        {{ trx_json_extract('src', ['WSR_PROCESS'], type='varchar', alias='wsr_process') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wsrsphist') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wsr_message', 'wsr_process']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wsr_lastsaved']) }}
