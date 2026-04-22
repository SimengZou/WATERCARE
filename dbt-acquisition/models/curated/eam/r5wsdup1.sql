{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSDUP1',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WD1_LASTSAVED'], type='timestamp_ntz', alias='wd1_lastsaved') }},
        {{ trx_json_extract('src', ['WD1_TIME'], type='timestamp_ntz', alias='wd1_time') }},
        {{ trx_json_extract('src', ['WD1_CODE'], type='varchar', alias='wd1_code') }},
        {{ trx_json_extract('src', ['WD1_UPDATECOUNT'], type='float', alias='wd1_updatecount') }},
        {{ trx_json_extract('src', ['WD1_DESC'], type='varchar', alias='wd1_desc') }},
        {{ trx_json_extract('src', ['WD1_TYPE'], type='varchar', alias='wd1_type') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wsdup1') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wd1_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wd1_lastsaved']) }}
