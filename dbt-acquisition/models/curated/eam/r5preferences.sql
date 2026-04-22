{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PREFERENCES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PRF_LASTSAVED'], type='timestamp_ntz', alias='prf_lastsaved') }},
        {{ trx_json_extract('src', ['PRF_NR'], type='float', alias='prf_nr') }},
        {{ trx_json_extract('src', ['PRF_UPDATECOUNT'], type='float', alias='prf_updatecount') }},
        {{ trx_json_extract('src', ['PRF_CODE'], type='varchar', alias='prf_code') }},
        {{ trx_json_extract('src', ['PRF_DEFAULT'], type='varchar', alias='prf_default') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5preferences') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['prf_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['prf_lastsaved']) }}
