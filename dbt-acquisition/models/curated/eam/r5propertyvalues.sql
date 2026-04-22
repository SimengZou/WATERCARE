{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PROPERTYVALUES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PRV_LASTSAVED'], type='timestamp_ntz', alias='prv_lastsaved') }},
        {{ trx_json_extract('src', ['PRV_RENTITY'], type='varchar', alias='prv_rentity') }},
        {{ trx_json_extract('src', ['PRV_CLASS'], type='varchar', alias='prv_class') }},
        {{ trx_json_extract('src', ['PRV_CODE'], type='varchar', alias='prv_code') }},
        {{ trx_json_extract('src', ['PRV_SEQNO'], type='float', alias='prv_seqno') }},
        {{ trx_json_extract('src', ['PRV_VALUE'], type='varchar', alias='prv_value') }},
        {{ trx_json_extract('src', ['PRV_DVALUE'], type='timestamp_ntz', alias='prv_dvalue') }},
        {{ trx_json_extract('src', ['PRV_CLASS_ORG'], type='varchar', alias='prv_class_org') }},
        {{ trx_json_extract('src', ['PRV_UPDATECOUNT'], type='float', alias='prv_updatecount') }},
        {{ trx_json_extract('src', ['PRV_CREATED'], type='timestamp_ntz', alias='prv_created') }},
        {{ trx_json_extract('src', ['PRV_UPDATED'], type='timestamp_ntz', alias='prv_updated') }},
        {{ trx_json_extract('src', ['PRV_NOTUSED'], type='varchar', alias='prv_notused') }},
        {{ trx_json_extract('src', ['PRV_PROPERTY'], type='varchar', alias='prv_property') }},
        {{ trx_json_extract('src', ['PRV_NVALUE'], type='float', alias='prv_nvalue') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5propertyvalues') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['prv_property', 'prv_rentity', 'prv_class', 'prv_class_org', 'prv_code', 'prv_seqno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['prv_lastsaved']) }}
