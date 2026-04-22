{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Auto-generated model',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['Version'], type='varchar', alias='version') }},
        {{ trx_json_extract('src', ['FinancialYear'], type='varchar', alias='financialyear') }},
        {{ trx_json_extract('src', ['Period'], type='varchar', alias='period') }},
        {{ trx_json_extract('src', ['Project'], type='varchar', alias='project') }},
        {{ trx_json_extract('src', ['WBS'], type='varchar', alias='wbs') }},
        {{ trx_json_extract('src', ['PurchaseOrder'], type='varchar', alias='purchaseorder') }},
        {{ trx_json_extract('src', ['PM101'], type='varchar', alias='pm101') }},
        {{ trx_json_extract('src', ['Value'], type='varchar', alias='value') }},
        {{ trx_json_extract('src', ['Timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_p101') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['version', 'financialyear', 'period', 'project', 'wbs', 'purchaseorder', 'pm101', 'value']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['timestamp']) }}
