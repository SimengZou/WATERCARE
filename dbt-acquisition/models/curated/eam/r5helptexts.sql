{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5HELPTEXTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['HEL_LASTSAVED'], type='timestamp_ntz', alias='hel_lastsaved') }},
        {{ trx_json_extract('src', ['HEL_ITEM'], type='varchar', alias='hel_item') }},
        {{ trx_json_extract('src', ['HEL_TEXT'], type='varchar', alias='hel_text') }},
        {{ trx_json_extract('src', ['HEL_LANG'], type='varchar', alias='hel_lang') }},
        {{ trx_json_extract('src', ['HEL_DRILLDOWN'], type='varchar', alias='hel_drilldown') }},
        {{ trx_json_extract('src', ['HEL_CHANGED'], type='varchar', alias='hel_changed') }},
        {{ trx_json_extract('src', ['HEL_UPDATECOUNT'], type='float', alias='hel_updatecount') }},
        {{ trx_json_extract('src', ['HEL_FUNCTION'], type='varchar', alias='hel_function') }},
        {{ trx_json_extract('src', ['HEL_POOL'], type='varchar', alias='hel_pool') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5helptexts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['hel_function', 'hel_item', 'hel_lang']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['hel_lastsaved']) }}
