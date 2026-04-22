{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5AUDITPK',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['APK_LASTSAVED'], type='timestamp_ntz', alias='apk_lastsaved') }},
        {{ trx_json_extract('src', ['APK_SEQNO'], type='float', alias='apk_seqno') }},
        {{ trx_json_extract('src', ['APK_COLUMN'], type='varchar', alias='apk_column') }},
        {{ trx_json_extract('src', ['APK_DATATYPE'], type='varchar', alias='apk_datatype') }},
        {{ trx_json_extract('src', ['APK_UPDATED'], type='timestamp_ntz', alias='apk_updated') }},
        {{ trx_json_extract('src', ['APK_TABLE'], type='varchar', alias='apk_table') }},
        {{ trx_json_extract('src', ['APK_UPDATECOUNT'], type='float', alias='apk_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5auditpk') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['apk_table', 'apk_seqno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['apk_lastsaved']) }}
