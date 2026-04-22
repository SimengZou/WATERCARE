{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERDEFINEDFIELDLOVVALS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UDL_LASTSAVED'], type='timestamp_ntz', alias='udl_lastsaved') }},
        {{ trx_json_extract('src', ['UDL_FIELD'], type='varchar', alias='udl_field') }},
        {{ trx_json_extract('src', ['UDL_CODE'], type='varchar', alias='udl_code') }},
        {{ trx_json_extract('src', ['UDL_UPDATED'], type='timestamp_ntz', alias='udl_updated') }},
        {{ trx_json_extract('src', ['UDL_UPDATECOUNT'], type='float', alias='udl_updatecount') }},
        {{ trx_json_extract('src', ['UDL_NOTUSED'], type='varchar', alias='udl_notused') }},
        {{ trx_json_extract('src', ['UDL_RENTITY'], type='varchar', alias='udl_rentity') }},
        {{ trx_json_extract('src', ['UDL_DESC'], type='varchar', alias='udl_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userdefinedfieldlovvals') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['udl_rentity', 'udl_field', 'udl_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['udl_lastsaved']) }}
