{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5SERVICEREQUEST',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CRM_PROBLEMCODE'], type='varchar', alias='crm_problemcode') }},
        {{ trx_json_extract('src', ['CRM_SERVICEREQUEST'], type='varchar', alias='crm_servicerequest') }},
        {{ trx_json_extract('src', ['CRM_PRIORITY'], type='varchar', alias='crm_priority') }},
        {{ trx_json_extract('src', ['CRM_ADDRESSKEY'], type='varchar', alias='crm_addresskey') }},
        {{ trx_json_extract('src', ['CRM_FLATNO'], type='varchar', alias='crm_flatno') }},
        {{ trx_json_extract('src', ['CRM_STREETNO'], type='varchar', alias='crm_streetno') }},
        {{ trx_json_extract('src', ['CRM_STNAME'], type='varchar', alias='crm_stname') }},
        {{ trx_json_extract('src', ['CRM_SUBURB'], type='varchar', alias='crm_suburb') }},
        {{ trx_json_extract('src', ['CRM_CITY'], type='varchar', alias='crm_city') }},
        {{ trx_json_extract('src', ['CRM_POSTALCODE'], type='varchar', alias='crm_postalcode') }},
        {{ trx_json_extract('src', ['CRM_NOTES'], type='varchar', alias='crm_notes') }},
        {{ trx_json_extract('src', ['CRM_REPORTEDDATE'], type='timestamp_ntz', alias='crm_reporteddate') }},
        {{ trx_json_extract('src', ['CRM_CALLTAKENBY'], type='varchar', alias='crm_calltakenby') }},
        {{ trx_json_extract('src', ['CRM_CALLERFISTNAME'], type='varchar', alias='crm_callerfistname') }},
        {{ trx_json_extract('src', ['CRM_CALLERLASTNAME'], type='varchar', alias='crm_callerlastname') }},
        {{ trx_json_extract('src', ['CRM_CONTACTNUMBER'], type='varchar', alias='crm_contactnumber') }},
        {{ trx_json_extract('src', ['CRM_ALTERNATECONTACT'], type='varchar', alias='crm_alternatecontact') }},
        {{ trx_json_extract('src', ['CRM_CONTACTEMAIL'], type='varchar', alias='crm_contactemail') }},
        {{ trx_json_extract('src', ['CRM_CONTACTREQUESTED'], type='varchar', alias='crm_contactrequested') }},
        {{ trx_json_extract('src', ['CRM_EVENT'], type='varchar', alias='crm_event') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['CRM_SOURCE'], type='varchar', alias='crm_source') }},
        {{ trx_json_extract('src', ['CRM_TRANSACTIONID'], type='varchar', alias='crm_transactionid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5servicerequest') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['crm_transactionid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
