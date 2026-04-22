{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Generated from anySQL Connector eam_r5events1',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['evt_code'], type='varchar', alias='evt_code') }},
        {{ trx_json_extract('src', ['evt_increment'], type='float', alias='evt_increment') }},
        {{ trx_json_extract('src', ['evt_status'], type='varchar', alias='evt_status') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5events1') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['evt_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_load_datetime']) }}
