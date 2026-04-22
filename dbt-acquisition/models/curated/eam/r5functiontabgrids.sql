{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FUNCTIONTABGRIDS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FTG_LASTSAVED'], type='timestamp_ntz', alias='ftg_lastsaved') }},
        {{ trx_json_extract('src', ['FTG_GRIDID'], type='float', alias='ftg_gridid') }},
        {{ trx_json_extract('src', ['FTG_FUNCTION'], type='varchar', alias='ftg_function') }},
        {{ trx_json_extract('src', ['FTG_TAB'], type='varchar', alias='ftg_tab') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5functiontabgrids') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ftg_function', 'ftg_tab', 'ftg_gridid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ftg_lastsaved']) }}
