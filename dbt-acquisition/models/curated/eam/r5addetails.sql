{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ADDETAILS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ADD_LASTSAVED'], type='timestamp_ntz', alias='add_lastsaved') }},
        {{ trx_json_extract('src', ['ADD_TYPE'], type='varchar', alias='add_type') }},
        {{ trx_json_extract('src', ['ADD_RTYPE'], type='varchar', alias='add_rtype') }},
        {{ trx_json_extract('src', ['ADD_CODE'], type='varchar', alias='add_code') }},
        {{ trx_json_extract('src', ['ADD_LANG'], type='varchar', alias='add_lang') }},
        {{ trx_json_extract('src', ['ADD_LINE'], type='float', alias='add_line') }},
        {{ trx_json_extract('src', ['ADD_PRINT'], type='varchar', alias='add_print') }},
        {{ trx_json_extract('src', ['ADD_TEXT'], type='varchar', alias='add_text') }},
        {{ trx_json_extract('src', ['ADD_CREATED'], type='timestamp_ntz', alias='add_created') }},
        {{ trx_json_extract('src', ['ADD_USER'], type='varchar', alias='add_user') }},
        {{ trx_json_extract('src', ['ADD_UPDATED'], type='timestamp_ntz', alias='add_updated') }},
        {{ trx_json_extract('src', ['ADD_UPDUSER'], type='varchar', alias='add_upduser') }},
        {{ trx_json_extract('src', ['ADD_UPDATECOUNT'], type='float', alias='add_updatecount') }},
        {{ trx_json_extract('src', ['ADD_ENTITY'], type='varchar', alias='add_entity') }},
        {{ trx_json_extract('src', ['ADD_RENTITY'], type='varchar', alias='add_rentity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5addetails') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['add_code', 'add_rentity', 'add_type', 'add_lang', 'add_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['add_lastsaved']) }}
