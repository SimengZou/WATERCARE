{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5JOBPARAMVALUE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['JPV_LASTSAVED'], type='timestamp_ntz', alias='jpv_lastsaved') }},
        {{ trx_json_extract('src', ['JPV_LINE'], type='float', alias='jpv_line') }},
        {{ trx_json_extract('src', ['JPV_VALUE'], type='varchar', alias='jpv_value') }},
        {{ trx_json_extract('src', ['JPV_UPDATECOUNT'], type='float', alias='jpv_updatecount') }},
        {{ trx_json_extract('src', ['JPV_CODE'], type='varchar', alias='jpv_code') }},
        {{ trx_json_extract('src', ['JPV_KEY'], type='varchar', alias='jpv_key') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5jobparamvalue') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['jpv_code', 'jpv_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['jpv_lastsaved']) }}
