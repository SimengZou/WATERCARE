{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DLVWSPROMPTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WST_LASTSAVED'], type='timestamp_ntz', alias='wst_lastsaved') }},
        {{ trx_json_extract('src', ['WST_DESC'], type='varchar', alias='wst_desc') }},
        {{ trx_json_extract('src', ['WST_NOTUSED'], type='varchar', alias='wst_notused') }},
        {{ trx_json_extract('src', ['WST_UPDATECOUNT'], type='float', alias='wst_updatecount') }},
        {{ trx_json_extract('src', ['WST_FUNCTION'], type='varchar', alias='wst_function') }},
        {{ trx_json_extract('src', ['WST_TAB'], type='varchar', alias='wst_tab') }},
        {{ trx_json_extract('src', ['WST_SYSTEM'], type='varchar', alias='wst_system') }},
        {{ trx_json_extract('src', ['WST_CODE'], type='varchar', alias='wst_code') }},
        {{ trx_json_extract('src', ['WST_UPDATED'], type='timestamp_ntz', alias='wst_updated') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dlvwsprompts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wst_lastsaved']) }}
