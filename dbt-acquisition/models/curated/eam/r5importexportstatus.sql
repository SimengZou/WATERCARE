{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IMPORTEXPORTSTATUS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IES_LASTSAVED'], type='timestamp_ntz', alias='ies_lastsaved') }},
        {{ trx_json_extract('src', ['IES_TYPE'], type='varchar', alias='ies_type') }},
        {{ trx_json_extract('src', ['IES_STATUS'], type='varchar', alias='ies_status') }},
        {{ trx_json_extract('src', ['IES_FILELOCATION'], type='varchar', alias='ies_filelocation') }},
        {{ trx_json_extract('src', ['IES_EMAILSENT'], type='varchar', alias='ies_emailsent') }},
        {{ trx_json_extract('src', ['IES_FILESENT'], type='varchar', alias='ies_filesent') }},
        {{ trx_json_extract('src', ['IES_ESTTIMETOSTART'], type='timestamp_ntz', alias='ies_esttimetostart') }},
        {{ trx_json_extract('src', ['IES_ESTTIMETOEND'], type='timestamp_ntz', alias='ies_esttimetoend') }},
        {{ trx_json_extract('src', ['IES_DATECREATED'], type='timestamp_ntz', alias='ies_datecreated') }},
        {{ trx_json_extract('src', ['IES_UPDATECOUNT'], type='float', alias='ies_updatecount') }},
        {{ trx_json_extract('src', ['IES_STARTED'], type='timestamp_ntz', alias='ies_started') }},
        {{ trx_json_extract('src', ['IES_COMPLETED'], type='timestamp_ntz', alias='ies_completed') }},
        {{ trx_json_extract('src', ['IES_RECORDCOUNT'], type='float', alias='ies_recordcount') }},
        {{ trx_json_extract('src', ['IES_EMAIL'], type='varchar', alias='ies_email') }},
        {{ trx_json_extract('src', ['IES_DESC'], type='varchar', alias='ies_desc') }},
        {{ trx_json_extract('src', ['IES_INCLUDEEMAIL'], type='varchar', alias='ies_includeemail') }},
        {{ trx_json_extract('src', ['IES_PREVIEW'], type='varchar', alias='ies_preview') }},
        {{ trx_json_extract('src', ['IES_USERID'], type='varchar', alias='ies_userid') }},
        {{ trx_json_extract('src', ['IES_SESSIONID'], type='float', alias='ies_sessionid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5importexportstatus') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ies_sessionid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ies_lastsaved']) }}
