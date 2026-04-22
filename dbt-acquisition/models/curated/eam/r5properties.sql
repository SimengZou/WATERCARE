{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PROPERTIES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PRO_LASTSAVED'], type='timestamp_ntz', alias='pro_lastsaved') }},
        {{ trx_json_extract('src', ['PRO_TYPE'], type='varchar', alias='pro_type') }},
        {{ trx_json_extract('src', ['PRO_TEXT'], type='varchar', alias='pro_text') }},
        {{ trx_json_extract('src', ['PRO_MIN'], type='varchar', alias='pro_min') }},
        {{ trx_json_extract('src', ['PRO_MAX'], type='varchar', alias='pro_max') }},
        {{ trx_json_extract('src', ['PRO_UPDATECOUNT'], type='float', alias='pro_updatecount') }},
        {{ trx_json_extract('src', ['PRO_CREATED'], type='timestamp_ntz', alias='pro_created') }},
        {{ trx_json_extract('src', ['PRO_UPDATED'], type='timestamp_ntz', alias='pro_updated') }},
        {{ trx_json_extract('src', ['PRO_INCLUDEINGRIDS'], type='varchar', alias='pro_includeingrids') }},
        {{ trx_json_extract('src', ['PRO_CODE'], type='varchar', alias='pro_code') }},
        {{ trx_json_extract('src', ['PRO_RENTITY'], type='varchar', alias='pro_rentity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5properties') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pro_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pro_lastsaved']) }}
