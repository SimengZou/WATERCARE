{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SOAUOMS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SUO_LASTSAVED'], type='timestamp_ntz', alias='suo_lastsaved') }},
        {{ trx_json_extract('src', ['SUO_DESC'], type='varchar', alias='suo_desc') }},
        {{ trx_json_extract('src', ['SUO_UPDATECOUNT'], type='float', alias='suo_updatecount') }},
        {{ trx_json_extract('src', ['SUO_CODE'], type='varchar', alias='suo_code') }},
        {{ trx_json_extract('src', ['SUO_ACTIVE'], type='varchar', alias='suo_active') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5soauoms') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['suo_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['suo_lastsaved']) }}
