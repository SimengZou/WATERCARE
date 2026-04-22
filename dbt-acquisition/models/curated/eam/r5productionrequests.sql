{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PRODUCTIONREQUESTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PRQ_LASTSAVED'], type='timestamp_ntz', alias='prq_lastsaved') }},
        {{ trx_json_extract('src', ['PRQ_ORG'], type='varchar', alias='prq_org') }},
        {{ trx_json_extract('src', ['PRQ_CLASS'], type='varchar', alias='prq_class') }},
        {{ trx_json_extract('src', ['PRQ_CLASS_ORG'], type='varchar', alias='prq_class_org') }},
        {{ trx_json_extract('src', ['PRQ_ACCOUNTINGENTITY'], type='varchar', alias='prq_accountingentity') }},
        {{ trx_json_extract('src', ['PRQ_STATUS'], type='varchar', alias='prq_status') }},
        {{ trx_json_extract('src', ['PRQ_RSTATUS'], type='varchar', alias='prq_rstatus') }},
        {{ trx_json_extract('src', ['PRQ_EVENT'], type='varchar', alias='prq_event') }},
        {{ trx_json_extract('src', ['PRQ_CREATED'], type='timestamp_ntz', alias='prq_created') }},
        {{ trx_json_extract('src', ['PRQ_CREATEDBY'], type='varchar', alias='prq_createdby') }},
        {{ trx_json_extract('src', ['PRQ_PRODREQUESTSTART'], type='timestamp_ntz', alias='prq_prodrequeststart') }},
        {{ trx_json_extract('src', ['PRQ_PRODREQUESTEND'], type='timestamp_ntz', alias='prq_prodrequestend') }},
        {{ trx_json_extract('src', ['PRQ_PRODUCTIONSTART'], type='timestamp_ntz', alias='prq_productionstart') }},
        {{ trx_json_extract('src', ['PRQ_PRODUCTIONEND'], type='timestamp_ntz', alias='prq_productionend') }},
        {{ trx_json_extract('src', ['PRQ_PRIORITY'], type='varchar', alias='prq_priority') }},
        {{ trx_json_extract('src', ['PRQ_PRODUCTIONORDER'], type='varchar', alias='prq_productionorder') }},
        {{ trx_json_extract('src', ['PRQ_ENTERPRISELOCATION'], type='varchar', alias='prq_enterpriselocation') }},
        {{ trx_json_extract('src', ['PRQ_LASTSTATUSUPDATE'], type='timestamp_ntz', alias='prq_laststatusupdate') }},
        {{ trx_json_extract('src', ['PRQ_UPDATECOUNT'], type='float', alias='prq_updatecount') }},
        {{ trx_json_extract('src', ['PRQ_REVISIONREASON'], type='varchar', alias='prq_revisionreason') }},
        {{ trx_json_extract('src', ['PRQ_CODE'], type='varchar', alias='prq_code') }},
        {{ trx_json_extract('src', ['PRQ_REVISION'], type='float', alias='prq_revision') }},
        {{ trx_json_extract('src', ['PRQ_DESC'], type='varchar', alias='prq_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5productionrequests') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['prq_code', 'prq_revision', 'prq_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['prq_lastsaved']) }}
