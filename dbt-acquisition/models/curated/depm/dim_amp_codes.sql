{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Auto-generated model',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ElementName'], type='varchar', alias='elementname') }},
        {{ trx_json_extract('src', ['Parent'], type='varchar', alias='parent') }},
        {{ trx_json_extract('src', ['Name'], type='varchar', alias='name') }},
        {{ trx_json_extract('src', ['ShortName'], type='varchar', alias='shortname') }},
        {{ trx_json_extract('src', ['Active'], type='varchar', alias='active') }},
        {{ trx_json_extract('src', ['Timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_dim_amp_codes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['elementname', 'parent']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['timestamp']) }}
