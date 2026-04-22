{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CONNLOCK',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CLK_LASTSAVED'], type='timestamp_ntz', alias='clk_lastsaved') }},
        {{ trx_json_extract('src', ['CLK_SESSIONID'], type='varchar', alias='clk_sessionid') }},
        {{ trx_json_extract('src', ['CLK_OPERATIONID'], type='varchar', alias='clk_operationid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5connlock') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['clk_operationid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['clk_lastsaved']) }}
