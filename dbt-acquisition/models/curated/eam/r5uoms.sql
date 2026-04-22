{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5UOMS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UOM_LASTSAVED'], type='timestamp_ntz', alias='uom_lastsaved') }},
        {{ trx_json_extract('src', ['UOM_DESC'], type='varchar', alias='uom_desc') }},
        {{ trx_json_extract('src', ['UOM_CLASS'], type='varchar', alias='uom_class') }},
        {{ trx_json_extract('src', ['UOM_NOTUSED'], type='varchar', alias='uom_notused') }},
        {{ trx_json_extract('src', ['UOM_UPDATECOUNT'], type='float', alias='uom_updatecount') }},
        {{ trx_json_extract('src', ['UOM_CREATED'], type='timestamp_ntz', alias='uom_created') }},
        {{ trx_json_extract('src', ['UOM_UPDATED'], type='timestamp_ntz', alias='uom_updated') }},
        {{ trx_json_extract('src', ['UOM_SOAUOM'], type='varchar', alias='uom_soauom') }},
        {{ trx_json_extract('src', ['UOM_CODE'], type='varchar', alias='uom_code') }},
        {{ trx_json_extract('src', ['UOM_CLASS_ORG'], type='varchar', alias='uom_class_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5uoms') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['uom_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['uom_lastsaved']) }}
