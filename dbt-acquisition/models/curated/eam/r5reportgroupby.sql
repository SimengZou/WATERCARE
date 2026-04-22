{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REPORTGROUPBY',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RGP_LASTSAVED'], type='timestamp_ntz', alias='rgp_lastsaved') }},
        {{ trx_json_extract('src', ['RGP_LINE'], type='float', alias='rgp_line') }},
        {{ trx_json_extract('src', ['RGP_GROUPFIELDS'], type='varchar', alias='rgp_groupfields') }},
        {{ trx_json_extract('src', ['RGP_UPDATECOUNT'], type='float', alias='rgp_updatecount') }},
        {{ trx_json_extract('src', ['RGP_UPDATED'], type='timestamp_ntz', alias='rgp_updated') }},
        {{ trx_json_extract('src', ['RGP_FUNCTION'], type='varchar', alias='rgp_function') }},
        {{ trx_json_extract('src', ['RGP_DEFAULT'], type='varchar', alias='rgp_default') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5reportgroupby') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rgp_function', 'rgp_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rgp_lastsaved']) }}
