{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ERRTEXTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ERT_LASTSAVED'], type='timestamp_ntz', alias='ert_lastsaved') }},
        {{ trx_json_extract('src', ['ERT_TEXT'], type='varchar', alias='ert_text') }},
        {{ trx_json_extract('src', ['ERT_TRANSLATE'], type='varchar', alias='ert_translate') }},
        {{ trx_json_extract('src', ['ERT_UPDATECOUNT'], type='float', alias='ert_updatecount') }},
        {{ trx_json_extract('src', ['ERT_CODE'], type='varchar', alias='ert_code') }},
        {{ trx_json_extract('src', ['ERT_LANG'], type='varchar', alias='ert_lang') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5errtexts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ert_code', 'ert_lang']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ert_lastsaved']) }}
