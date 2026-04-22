{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWORGANIZATIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZOR_LASTSAVED'], type='timestamp_ntz', alias='zor_lastsaved') }},
        {{ trx_json_extract('src', ['ZOR_CODE'], type='varchar', alias='zor_code') }},
        {{ trx_json_extract('src', ['ZOR_DESC'], type='varchar', alias='zor_desc') }},
        {{ trx_json_extract('src', ['ZOR_KEY'], type='float', alias='zor_key') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dworganizations') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['zor_lastsaved']) }}
