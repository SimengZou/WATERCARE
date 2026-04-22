{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MAILTEMPLATE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MAT_LASTSAVED'], type='timestamp_ntz', alias='mat_lastsaved') }},
        {{ trx_json_extract('src', ['MAT_SUBJECT'], type='varchar', alias='mat_subject') }},
        {{ trx_json_extract('src', ['MAT_TEXT'], type='varchar', alias='mat_text') }},
        {{ trx_json_extract('src', ['MAT_MAIL'], type='varchar', alias='mat_mail') }},
        {{ trx_json_extract('src', ['MAT_UPDATECOUNT'], type='float', alias='mat_updatecount') }},
        {{ trx_json_extract('src', ['MAT_CODE'], type='varchar', alias='mat_code') }},
        {{ trx_json_extract('src', ['MAT_FROMEMAIL'], type='varchar', alias='mat_fromemail') }},
        {{ trx_json_extract('src', ['MAT_PUSHNOTIFICATION'], type='varchar', alias='mat_pushnotification') }},
        {{ trx_json_extract('src', ['MAT_EMAIL'], type='varchar', alias='mat_email') }},
        {{ trx_json_extract('src', ['MAT_NOTEBOOKEMAILS'], type='varchar', alias='mat_notebookemails') }},
        {{ trx_json_extract('src', ['MAT_DESC'], type='varchar', alias='mat_desc') }},
        {{ trx_json_extract('src', ['MAT_REPORT'], type='varchar', alias='mat_report') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mailtemplate') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mat_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mat_lastsaved']) }}
