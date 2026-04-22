{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5AUDATTRIBS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AAT_LASTSAVED'], type='timestamp_ntz', alias='aat_lastsaved') }},
        {{ trx_json_extract('src', ['AAT_TABLE'], type='varchar', alias='aat_table') }},
        {{ trx_json_extract('src', ['AAT_COLUMN'], type='varchar', alias='aat_column') }},
        {{ trx_json_extract('src', ['AAT_TEXT'], type='varchar', alias='aat_text') }},
        {{ trx_json_extract('src', ['AAT_ENTEREDBY'], type='varchar', alias='aat_enteredby') }},
        {{ trx_json_extract('src', ['AAT_INSERT'], type='varchar', alias='aat_insert') }},
        {{ trx_json_extract('src', ['AAT_UPDATE'], type='varchar', alias='aat_update') }},
        {{ trx_json_extract('src', ['AAT_DELETE'], type='varchar', alias='aat_delete') }},
        {{ trx_json_extract('src', ['AAT_UPDATECOUNT'], type='float', alias='aat_updatecount') }},
        {{ trx_json_extract('src', ['AAT_UPDATED'], type='timestamp_ntz', alias='aat_updated') }},
        {{ trx_json_extract('src', ['AAT_CODE'], type='float', alias='aat_code') }},
        {{ trx_json_extract('src', ['AAT_COMMENT'], type='varchar', alias='aat_comment') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5audattribs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['aat_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['aat_lastsaved']) }}
