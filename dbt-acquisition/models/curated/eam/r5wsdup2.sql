{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSDUP2',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WD2_LASTSAVED'], type='timestamp_ntz', alias='wd2_lastsaved') }},
        {{ trx_json_extract('src', ['WD2_DESC'], type='varchar', alias='wd2_desc') }},
        {{ trx_json_extract('src', ['WD2_TYPE'], type='varchar', alias='wd2_type') }},
        {{ trx_json_extract('src', ['WD2_UPDATECOUNT'], type='float', alias='wd2_updatecount') }},
        {{ trx_json_extract('src', ['WD2_CODE'], type='varchar', alias='wd2_code') }},
        {{ trx_json_extract('src', ['WD2_TIME'], type='timestamp_ntz', alias='wd2_time') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wsdup2') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wd2_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wd2_lastsaved']) }}
