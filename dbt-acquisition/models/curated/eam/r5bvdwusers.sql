{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BVDWUSERS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['USR_CODE'], type='varchar', alias='usr_code') }},
        {{ trx_json_extract('src', ['USR_DEFORG'], type='varchar', alias='usr_deforg') }},
        {{ trx_json_extract('src', ['USR_LASTSAVED'], type='timestamp_ntz', alias='usr_lastsaved') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5bvdwusers') }}
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
