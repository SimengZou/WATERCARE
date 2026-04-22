{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IMPORTEXPORTGRPSTATUS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IMG_LASTSAVED'], type='timestamp_ntz', alias='img_lastsaved') }},
        {{ trx_json_extract('src', ['IMG_GROUP'], type='varchar', alias='img_group') }},
        {{ trx_json_extract('src', ['IMG_STATUS'], type='varchar', alias='img_status') }},
        {{ trx_json_extract('src', ['IMG_START'], type='timestamp_ntz', alias='img_start') }},
        {{ trx_json_extract('src', ['IMG_END'], type='timestamp_ntz', alias='img_end') }},
        {{ trx_json_extract('src', ['IMG_SESSIONID'], type='float', alias='img_sessionid') }},
        {{ trx_json_extract('src', ['IMG_PROCESSORDER'], type='float', alias='img_processorder') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5importexportgrpstatus') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['img_sessionid', 'img_group']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['img_lastsaved']) }}
