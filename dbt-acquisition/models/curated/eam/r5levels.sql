{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5LEVELS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LVL_LASTSAVED'], type='timestamp_ntz', alias='lvl_lastsaved') }},
        {{ trx_json_extract('src', ['LVL_LEVEL'], type='float', alias='lvl_level') }},
        {{ trx_json_extract('src', ['LVL_UPDATECOUNT'], type='float', alias='lvl_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5levels') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lvl_lastsaved']) }}
