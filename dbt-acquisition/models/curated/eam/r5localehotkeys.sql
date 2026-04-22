{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5LOCALEHOTKEYS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LHK_LASTSAVED'], type='timestamp_ntz', alias='lhk_lastsaved') }},
        {{ trx_json_extract('src', ['LHK_CODE'], type='varchar', alias='lhk_code') }},
        {{ trx_json_extract('src', ['LHK_DEFAULT'], type='float', alias='lhk_default') }},
        {{ trx_json_extract('src', ['LHK_DESC'], type='varchar', alias='lhk_desc') }},
        {{ trx_json_extract('src', ['LHK_EXTRA'], type='varchar', alias='lhk_extra') }},
        {{ trx_json_extract('src', ['LHK_UPDATECOUNT'], type='float', alias='lhk_updatecount') }},
        {{ trx_json_extract('src', ['LHK_LOCALE'], type='varchar', alias='lhk_locale') }},
        {{ trx_json_extract('src', ['LHK_KEY'], type='float', alias='lhk_key') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5localehotkeys') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['lhk_locale', 'lhk_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lhk_lastsaved']) }}
