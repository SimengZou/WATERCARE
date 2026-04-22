{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5QUERIES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['QUE_LASTSAVED'], type='timestamp_ntz', alias='que_lastsaved') }},
        {{ trx_json_extract('src', ['QUE_TEXT'], type='varchar', alias='que_text') }},
        {{ trx_json_extract('src', ['QUE_NORMAL'], type='varchar', alias='que_normal') }},
        {{ trx_json_extract('src', ['QUE_ASSET'], type='varchar', alias='que_asset') }},
        {{ trx_json_extract('src', ['QUE_INBOX'], type='varchar', alias='que_inbox') }},
        {{ trx_json_extract('src', ['QUE_KPI'], type='varchar', alias='que_kpi') }},
        {{ trx_json_extract('src', ['QUE_UPDATECOUNT'], type='float', alias='que_updatecount') }},
        {{ trx_json_extract('src', ['QUE_UPDATED'], type='timestamp_ntz', alias='que_updated') }},
        {{ trx_json_extract('src', ['QUE_CHART'], type='varchar', alias='que_chart') }},
        {{ trx_json_extract('src', ['QUE_DESC'], type='varchar', alias='que_desc') }},
        {{ trx_json_extract('src', ['QUE_NOTE'], type='varchar', alias='que_note') }},
        {{ trx_json_extract('src', ['QUE_EQUIPMENTRANKING'], type='varchar', alias='que_equipmentranking') }},
        {{ trx_json_extract('src', ['QUE_CODE'], type='varchar', alias='que_code') }},
        {{ trx_json_extract('src', ['QUE_LOOKUP'], type='varchar', alias='que_lookup') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5queries') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['que_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['que_lastsaved']) }}
