{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PROCESSLOCK',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PLK_LASTSAVED'], type='timestamp_ntz', alias='plk_lastsaved') }},
        {{ trx_json_extract('src', ['PLK_DESC'], type='varchar', alias='plk_desc') }},
        {{ trx_json_extract('src', ['PLK_UPDATECOUNT'], type='float', alias='plk_updatecount') }},
        {{ trx_json_extract('src', ['PLK_CODE'], type='varchar', alias='plk_code') }},
        {{ trx_json_extract('src', ['PLK_LOCKTIME'], type='timestamp_ntz', alias='plk_locktime') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5processlock') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['plk_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['plk_lastsaved']) }}
