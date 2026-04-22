{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSREQHIST',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WSQ_LASTSAVED'], type='timestamp_ntz', alias='wsq_lastsaved') }},
        {{ trx_json_extract('src', ['WSQ_PROCESS'], type='varchar', alias='wsq_process') }},
        {{ trx_json_extract('src', ['WSQ_TIME'], type='timestamp_ntz', alias='wsq_time') }},
        {{ trx_json_extract('src', ['WSQ_STATUS'], type='varchar', alias='wsq_status') }},
        {{ trx_json_extract('src', ['WSQ_STATUS_MESSAGE'], type='varchar', alias='wsq_status_message') }},
        {{ trx_json_extract('src', ['WSQ_DATAAREA'], type='float', alias='wsq_dataarea') }},
        {{ trx_json_extract('src', ['WSQ_UPDATECOUNT'], type='float', alias='wsq_updatecount') }},
        {{ trx_json_extract('src', ['WSQ_MESSAGE'], type='varchar', alias='wsq_message') }},
        {{ trx_json_extract('src', ['WSQ_RSTATUS'], type='varchar', alias='wsq_rstatus') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wsreqhist') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wsq_message', 'wsq_process']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wsq_lastsaved']) }}
