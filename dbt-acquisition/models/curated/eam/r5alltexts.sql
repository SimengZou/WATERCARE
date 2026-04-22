{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALLTEXTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ATX_LASTSAVED'], type='timestamp_ntz', alias='atx_lastsaved') }},
        {{ trx_json_extract('src', ['ATX_TEXTTYPE'], type='varchar', alias='atx_texttype') }},
        {{ trx_json_extract('src', ['ATX_LANG'], type='varchar', alias='atx_lang') }},
        {{ trx_json_extract('src', ['ATX_LASTMODIFIED'], type='timestamp_ntz', alias='atx_lastmodified') }},
        {{ trx_json_extract('src', ['ATX_UPDATECOUNT'], type='float', alias='atx_updatecount') }},
        {{ trx_json_extract('src', ['ATX_CODE'], type='varchar', alias='atx_code') }},
        {{ trx_json_extract('src', ['ATX_TEXT'], type='varchar', alias='atx_text') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alltexts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['atx_code', 'atx_texttype', 'atx_lang']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['atx_lastsaved']) }}
