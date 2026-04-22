{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BVDWBOILERTEXTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['BOT_FUNCTION'], type='varchar', alias='bot_function') }},
        {{ trx_json_extract('src', ['BOT_LANG'], type='varchar', alias='bot_lang') }},
        {{ trx_json_extract('src', ['BOT_NUMBER'], type='float', alias='bot_number') }},
        {{ trx_json_extract('src', ['BOT_TEXT'], type='varchar', alias='bot_text') }},
        {{ trx_json_extract('src', ['BOT_LASTSAVED'], type='timestamp_ntz', alias='bot_lastsaved') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5bvdwboilertexts') }}
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
