{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BOILERTEXTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['BOT_LASTSAVED'], type='timestamp_ntz', alias='bot_lastsaved') }},
        {{ trx_json_extract('src', ['BOT_LENGTH'], type='float', alias='bot_length') }},
        {{ trx_json_extract('src', ['BOT_XPOS'], type='float', alias='bot_xpos') }},
        {{ trx_json_extract('src', ['BOT_YPOS'], type='float', alias='bot_ypos') }},
        {{ trx_json_extract('src', ['BOT_LANG'], type='varchar', alias='bot_lang') }},
        {{ trx_json_extract('src', ['BOT_TEXT'], type='varchar', alias='bot_text') }},
        {{ trx_json_extract('src', ['BOT_DEST'], type='varchar', alias='bot_dest') }},
        {{ trx_json_extract('src', ['BOT_PAGE'], type='varchar', alias='bot_page') }},
        {{ trx_json_extract('src', ['BOT_FLD1'], type='varchar', alias='bot_fld1') }},
        {{ trx_json_extract('src', ['BOT_FLD2'], type='varchar', alias='bot_fld2') }},
        {{ trx_json_extract('src', ['BOT_PRTP'], type='varchar', alias='bot_prtp') }},
        {{ trx_json_extract('src', ['BOT_LVCR'], type='float', alias='bot_lvcr') }},
        {{ trx_json_extract('src', ['BOT_ADLEN'], type='float', alias='bot_adlen') }},
        {{ trx_json_extract('src', ['BOT_POOL'], type='varchar', alias='bot_pool') }},
        {{ trx_json_extract('src', ['BOT_CHANGED'], type='varchar', alias='bot_changed') }},
        {{ trx_json_extract('src', ['BOT_DISPLAY'], type='varchar', alias='bot_display') }},
        {{ trx_json_extract('src', ['BOT_UPDATECOUNT'], type='float', alias='bot_updatecount') }},
        {{ trx_json_extract('src', ['BOT_CREATED'], type='timestamp_ntz', alias='bot_created') }},
        {{ trx_json_extract('src', ['BOT_UPDATED'], type='timestamp_ntz', alias='bot_updated') }},
        {{ trx_json_extract('src', ['BOT_CLONED'], type='varchar', alias='bot_cloned') }},
        {{ trx_json_extract('src', ['BOT_FUNCTION'], type='varchar', alias='bot_function') }},
        {{ trx_json_extract('src', ['BOT_NUMBER'], type='float', alias='bot_number') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5boilertexts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['bot_function', 'bot_number', 'bot_lang']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['bot_lastsaved']) }}
