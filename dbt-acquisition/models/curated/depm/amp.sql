{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='AMP Cube Data',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AMPVersion'], type='varchar', alias='ampversion') }},
        {{ trx_json_extract('src', ['AMPYear'], type='varchar', alias='ampyear') }},
        {{ trx_json_extract('src', ['AMPPeriod'], type='varchar', alias='ampperiod') }},
        {{ trx_json_extract('src', ['AMPProject'], type='varchar', alias='ampproject') }},
        {{ trx_json_extract('src', ['AMPBusinessArea'], type='varchar', alias='ampbusinessarea') }},
        {{ trx_json_extract('src', ['AMPcodes'], type='varchar', alias='ampcodes') }},
        {{ trx_json_extract('src', ['AMPEMCodes'], type='varchar', alias='ampemcodes') }},
        {{ trx_json_extract('src', ['AMPMeas'], type='varchar', alias='ampmeas') }},
        {{ trx_json_extract('src', ['Value'], type='varchar', alias='value') }},
        {{ trx_json_extract('src', ['Timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_amp') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ampversion', 'ampyear', 'ampperiod', 'ampproject', 'ampbusinessarea', 'ampcodes', 'ampemcodes', 'ampmeas']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['timestamp']) }}
