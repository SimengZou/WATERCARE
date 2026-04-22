{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5AGREEMENTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AGR_LASTSAVED'], type='timestamp_ntz', alias='agr_lastsaved') }},
        {{ trx_json_extract('src', ['AGR_STATUS'], type='varchar', alias='agr_status') }},
        {{ trx_json_extract('src', ['AGR_RSTATUS'], type='varchar', alias='agr_rstatus') }},
        {{ trx_json_extract('src', ['AGR_CLASS'], type='varchar', alias='agr_class') }},
        {{ trx_json_extract('src', ['AGR_DEBTOR'], type='varchar', alias='agr_debtor') }},
        {{ trx_json_extract('src', ['AGR_ARRANGEMENT'], type='varchar', alias='agr_arrangement') }},
        {{ trx_json_extract('src', ['AGR_START'], type='timestamp_ntz', alias='agr_start') }},
        {{ trx_json_extract('src', ['AGR_END'], type='timestamp_ntz', alias='agr_end') }},
        {{ trx_json_extract('src', ['AGR_OBJECT'], type='varchar', alias='agr_object') }},
        {{ trx_json_extract('src', ['AGR_PROJECT'], type='varchar', alias='agr_project') }},
        {{ trx_json_extract('src', ['AGR_EVENT'], type='varchar', alias='agr_event') }},
        {{ trx_json_extract('src', ['AGR_ORG'], type='varchar', alias='agr_org') }},
        {{ trx_json_extract('src', ['AGR_CLASS_ORG'], type='varchar', alias='agr_class_org') }},
        {{ trx_json_extract('src', ['AGR_OBJECT_ORG'], type='varchar', alias='agr_object_org') }},
        {{ trx_json_extract('src', ['AGR_UPDATECOUNT'], type='float', alias='agr_updatecount') }},
        {{ trx_json_extract('src', ['AGR_CODE'], type='varchar', alias='agr_code') }},
        {{ trx_json_extract('src', ['AGR_DESC'], type='varchar', alias='agr_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5agreements') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['agr_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['agr_lastsaved']) }}
