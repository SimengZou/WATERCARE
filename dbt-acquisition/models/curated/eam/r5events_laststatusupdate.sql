{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EVENTS_LASTSTATUSUPDATE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ELU_LASTSAVED'], type='timestamp_ntz', alias='elu_lastsaved') }},
        {{ trx_json_extract('src', ['ELU_CODE'], type='varchar', alias='elu_code') }},
        {{ trx_json_extract('src', ['ELU_LASTSTATUSUPDATE'], type='timestamp_ntz', alias='elu_laststatusupdate') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5events_laststatusupdate') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['elu_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['elu_lastsaved']) }}
