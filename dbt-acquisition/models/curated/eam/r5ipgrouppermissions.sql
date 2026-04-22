{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IPGROUPPERMISSIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IPG_LASTSAVED'], type='timestamp_ntz', alias='ipg_lastsaved') }},
        {{ trx_json_extract('src', ['IPG_PERMISSION'], type='float', alias='ipg_permission') }},
        {{ trx_json_extract('src', ['IPG_ACTIVE'], type='varchar', alias='ipg_active') }},
        {{ trx_json_extract('src', ['IPG_GROUP'], type='varchar', alias='ipg_group') }},
        {{ trx_json_extract('src', ['IPG_UPDATECOUNT'], type='float', alias='ipg_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5ipgrouppermissions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ipg_group', 'ipg_permission']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ipg_lastsaved']) }}
