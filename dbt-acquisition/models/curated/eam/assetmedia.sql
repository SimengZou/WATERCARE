{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Auto-generated model',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compkey'], type='varchar', alias='compkey') }},
        {{ trx_json_extract('src', ['yskdate'], type='varchar', alias='yskdate') }},
        {{ trx_json_extract('src', ['org'], type='varchar', alias='org') }},
        {{ trx_json_extract('src', ['source'], type='varchar', alias='source') }},
        {{ trx_json_extract('src', ['documentid'], type='varchar', alias='documentid') }},
        {{ trx_json_extract('src', ['ticketreference'], type='varchar', alias='ticketreference') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_assetmedia') }}
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
