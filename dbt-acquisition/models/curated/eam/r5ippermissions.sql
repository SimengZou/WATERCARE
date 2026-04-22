{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IPPERMISSIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IPP_LASTSAVED'], type='timestamp_ntz', alias='ipp_lastsaved') }},
        {{ trx_json_extract('src', ['IPP_MNEMONIC'], type='varchar', alias='ipp_mnemonic') }},
        {{ trx_json_extract('src', ['IPP_UPDATECOUNT'], type='float', alias='ipp_updatecount') }},
        {{ trx_json_extract('src', ['IPP_CODE'], type='float', alias='ipp_code') }},
        {{ trx_json_extract('src', ['IPP_FUNCTION'], type='float', alias='ipp_function') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5ippermissions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ipp_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ipp_lastsaved']) }}
