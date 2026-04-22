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
        {{ trx_json_extract('src', ['Year'], type='varchar', alias='year') }},
        {{ trx_json_extract('src', ['Period'], type='varchar', alias='period') }},
        {{ trx_json_extract('src', ['Currency'], type='varchar', alias='currency') }},
        {{ trx_json_extract('src', ['Company'], type='varchar', alias='company') }},
        {{ trx_json_extract('src', ['Cost_Centre'], type='varchar', alias='cost_centre') }},
        {{ trx_json_extract('src', ['Asset_Group'], type='varchar', alias='asset_group') }},
        {{ trx_json_extract('src', ['Activity'], type='varchar', alias='activity') }},
        {{ trx_json_extract('src', ['Customer'], type='varchar', alias='customer') }},
        {{ trx_json_extract('src', ['Account'], type='varchar', alias='_account') }},
        {{ trx_json_extract('src', ['Value'], type='float', alias='value') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_b100') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_load_datetime']) }}
