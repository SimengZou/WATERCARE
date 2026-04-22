{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5WAREHOUSES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['WRH_CODE'], type='varchar', alias='wrh_code') }},
        {{ trx_json_extract('src', ['WRH_DESC'], type='varchar', alias='wrh_desc') }},
        {{ trx_json_extract('src', ['WRH_ENTERPRISELOCATION'], type='varchar', alias='wrh_enterpriselocation') }},
        {{ trx_json_extract('src', ['WRH_ACCOUNTINGENTITY'], type='varchar', alias='wrh_accountingentity') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['WRH_TRANS'], type='varchar', alias='wrh_trans') }},
        {{ trx_json_extract('src', ['WRH_LOCATIONS'], type='varchar', alias='wrh_locations') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5warehouses') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wrh_trans']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
