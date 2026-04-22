{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5WOCLAIMS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['WCO_TRANS_FLAG'], type='varchar', alias='wco_trans_flag') }},
        {{ trx_json_extract('src', ['WCO_ORG'], type='varchar', alias='wco_org') }},
        {{ trx_json_extract('src', ['WCO_SCHEDULE_ITEM'], type='varchar', alias='wco_schedule_item') }},
        {{ trx_json_extract('src', ['WCO_TRANSID'], type='varchar', alias='wco_transid') }},
        {{ trx_json_extract('src', ['WCO_CONTRACTOR_QTY'], type='float', alias='wco_contractor_qty') }},
        {{ trx_json_extract('src', ['WCO_CONTRACTOR_RATE'], type='float', alias='wco_contractor_rate') }},
        {{ trx_json_extract('src', ['WCO_CHARGEDATE'], type='timestamp_ntz', alias='wco_chargedate') }},
        {{ trx_json_extract('src', ['WCO_COMMENTS'], type='varchar', alias='wco_comments') }},
        {{ trx_json_extract('src', ['WCO_SCHEDITEM_DESC'], type='varchar', alias='wco_scheditem_desc') }},
        {{ trx_json_extract('src', ['WCO_SCHEDITEM_RATE'], type='float', alias='wco_scheditem_rate') }},
        {{ trx_json_extract('src', ['WCO_LINETOTAL'], type='float', alias='wco_linetotal') }},
        {{ trx_json_extract('src', ['WCO_WOSCHEDITEM'], type='varchar', alias='wco_woscheditem') }},
        {{ trx_json_extract('src', ['WCO_WOTYPE'], type='varchar', alias='wco_wotype') }},
        {{ trx_json_extract('src', ['WCO_WOPARENT'], type='varchar', alias='wco_woparent') }},
        {{ trx_json_extract('src', ['WCO_CONTRACT_CODE'], type='varchar', alias='wco_contract_code') }},
        {{ trx_json_extract('src', ['WCO_CONTRACTOR'], type='varchar', alias='wco_contractor') }},
        {{ trx_json_extract('src', ['WCO_ACTIVITY'], type='varchar', alias='wco_activity') }},
        {{ trx_json_extract('src', ['WCO_ACTIVITY_DESC'], type='varchar', alias='wco_activity_desc') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['WCO_LINE_STATUS'], type='varchar', alias='wco_line_status') }},
        {{ trx_json_extract('src', ['WCO_COMMENTS_INT'], type='varchar', alias='wco_comments_int') }},
        {{ trx_json_extract('src', ['WCO_DERPROJACT'], type='varchar', alias='wco_derprojact') }},
        {{ trx_json_extract('src', ['WCO_DERPROJNUM'], type='varchar', alias='wco_derprojnum') }},
        {{ trx_json_extract('src', ['WCO_EXPPROJACT'], type='varchar', alias='wco_expprojact') }},
        {{ trx_json_extract('src', ['WCO_EXPPROJNUM'], type='varchar', alias='wco_expprojnum') }},
        {{ trx_json_extract('src', ['WCO_PROCESSED_ERROR'], type='varchar', alias='wco_processed_error') }},
        {{ trx_json_extract('src', ['WCO_PROCESSED_STATUS'], type='varchar', alias='wco_processed_status') }},
        {{ trx_json_extract('src', ['WCO_EVENT'], type='varchar', alias='wco_event') }},
        {{ trx_json_extract('src', ['WCO_PK'], type='varchar', alias='wco_pk') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5woclaims') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wco_event', 'wco_pk', 'wco_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
