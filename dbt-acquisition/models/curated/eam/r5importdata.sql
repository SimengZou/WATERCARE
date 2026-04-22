{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IMPORTDATA',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IPD_LASTSAVED'], type='timestamp_ntz', alias='ipd_lastsaved') }},
        {{ trx_json_extract('src', ['IPD_LINE'], type='float', alias='ipd_line') }},
        {{ trx_json_extract('src', ['IPD_STATUS'], type='varchar', alias='ipd_status') }},
        {{ trx_json_extract('src', ['IPD_MESSAGE'], type='varchar', alias='ipd_message') }},
        {{ trx_json_extract('src', ['IPD_DATA'], type='varchar', alias='ipd_data') }},
        {{ trx_json_extract('src', ['IPD_DECODED'], type='varchar', alias='ipd_decoded') }},
        {{ trx_json_extract('src', ['IPD_UPDATED'], type='timestamp_ntz', alias='ipd_updated') }},
        {{ trx_json_extract('src', ['IPD_SESSIONID'], type='float', alias='ipd_sessionid') }},
        {{ trx_json_extract('src', ['IPD_TABLE'], type='varchar', alias='ipd_table') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5importdata') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ipd_lastsaved']) }}
