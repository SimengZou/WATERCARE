{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EVTSYSTEMS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ESY_LASTSAVED'], type='timestamp_ntz', alias='esy_lastsaved') }},
        {{ trx_json_extract('src', ['ESY_SYSTEM'], type='varchar', alias='esy_system') }},
        {{ trx_json_extract('src', ['ESY_UPDATECOUNT'], type='float', alias='esy_updatecount') }},
        {{ trx_json_extract('src', ['ESY_EVENT'], type='varchar', alias='esy_event') }},
        {{ trx_json_extract('src', ['ESY_SYSTEM_ORG'], type='varchar', alias='esy_system_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5evtsystems') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['esy_event', 'esy_system', 'esy_system_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['esy_lastsaved']) }}
