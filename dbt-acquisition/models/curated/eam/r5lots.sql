{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5LOTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LOT_LASTSAVED'], type='timestamp_ntz', alias='lot_lastsaved') }},
        {{ trx_json_extract('src', ['LOT_DESC'], type='varchar', alias='lot_desc') }},
        {{ trx_json_extract('src', ['LOT_CLASS'], type='varchar', alias='lot_class') }},
        {{ trx_json_extract('src', ['LOT_EXPIRED'], type='timestamp_ntz', alias='lot_expired') }},
        {{ trx_json_extract('src', ['LOT_MANLOT'], type='varchar', alias='lot_manlot') }},
        {{ trx_json_extract('src', ['LOT_CLASS_ORG'], type='varchar', alias='lot_class_org') }},
        {{ trx_json_extract('src', ['LOT_UPDATECOUNT'], type='float', alias='lot_updatecount') }},
        {{ trx_json_extract('src', ['LOT_BUILDKITTRANS'], type='varchar', alias='lot_buildkittrans') }},
        {{ trx_json_extract('src', ['LOT_BREAKUPKITTRANS'], type='varchar', alias='lot_breakupkittrans') }},
        {{ trx_json_extract('src', ['LOT_CODE'], type='varchar', alias='lot_code') }},
        {{ trx_json_extract('src', ['LOT_ORG'], type='varchar', alias='lot_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5lots') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['lot_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lot_lastsaved']) }}
