{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5POINTTYPES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PTP_LASTSAVED'], type='timestamp_ntz', alias='ptp_lastsaved') }},
        {{ trx_json_extract('src', ['PTP_DESC'], type='varchar', alias='ptp_desc') }},
        {{ trx_json_extract('src', ['PTP_CLASS'], type='varchar', alias='ptp_class') }},
        {{ trx_json_extract('src', ['PTP_UPDATECOUNT'], type='float', alias='ptp_updatecount') }},
        {{ trx_json_extract('src', ['PTP_CREATED'], type='timestamp_ntz', alias='ptp_created') }},
        {{ trx_json_extract('src', ['PTP_UPDATED'], type='timestamp_ntz', alias='ptp_updated') }},
        {{ trx_json_extract('src', ['PTP_CODE'], type='varchar', alias='ptp_code') }},
        {{ trx_json_extract('src', ['PTP_CLASS_ORG'], type='varchar', alias='ptp_class_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pointtypes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ptp_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ptp_lastsaved']) }}
