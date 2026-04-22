{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Capitalization Cube',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['Asset'], type='varchar', alias='asset') }},
        {{ trx_json_extract('src', ['Project'], type='varchar', alias='project') }},
        {{ trx_json_extract('src', ['AssetBook'], type='varchar', alias='assetbook') }},
        {{ trx_json_extract('src', ['CM100'], type='varchar', alias='cm100') }},
        {{ trx_json_extract('src', ['Value'], type='varchar', alias='value') }},
        {{ trx_json_extract('src', ['Timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_c100') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['asset', 'project', 'assetbook', 'cm100']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['timestamp']) }}
