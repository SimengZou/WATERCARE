{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MOBILETABLEUPDATES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MTU_LASTSAVED'], type='timestamp_ntz', alias='mtu_lastsaved') }},
        {{ trx_json_extract('src', ['MTU_RENTITY'], type='varchar', alias='mtu_rentity') }},
        {{ trx_json_extract('src', ['MTU_TABLENAME'], type='varchar', alias='mtu_tablename') }},
        {{ trx_json_extract('src', ['MTU_UPDATED'], type='timestamp_ntz', alias='mtu_updated') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mobiletableupdates') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mtu_lastsaved']) }}
