{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TRACKINGLOVS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TKV_LASTSAVED'], type='timestamp_ntz', alias='tkv_lastsaved') }},
        {{ trx_json_extract('src', ['TKV_SQL'], type='varchar', alias='tkv_sql') }},
        {{ trx_json_extract('src', ['TKV_UPDATECOUNT'], type='float', alias='tkv_updatecount') }},
        {{ trx_json_extract('src', ['TKV_CODE'], type='varchar', alias='tkv_code') }},
        {{ trx_json_extract('src', ['TKV_UPDATED'], type='timestamp_ntz', alias='tkv_updated') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5trackinglovs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['tkv_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['tkv_lastsaved']) }}
