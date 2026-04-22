{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5RCODES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RCO_LASTSAVED'], type='timestamp_ntz', alias='rco_lastsaved') }},
        {{ trx_json_extract('src', ['RCO_RCODE'], type='varchar', alias='rco_rcode') }},
        {{ trx_json_extract('src', ['RCO_UPDATECOUNT'], type='float', alias='rco_updatecount') }},
        {{ trx_json_extract('src', ['RCO_TEXTCODE'], type='varchar', alias='rco_textcode') }},
        {{ trx_json_extract('src', ['RCO_RENTITY'], type='varchar', alias='rco_rentity') }},
        {{ trx_json_extract('src', ['RCO_DESC'], type='varchar', alias='rco_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5rcodes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rco_rentity', 'rco_rcode']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rco_lastsaved']) }}
