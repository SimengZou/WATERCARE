{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EDITIONBACKREPS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EBR_LASTSAVED'], type='timestamp_ntz', alias='ebr_lastsaved') }},
        {{ trx_json_extract('src', ['EBR_REPORT'], type='varchar', alias='ebr_report') }},
        {{ trx_json_extract('src', ['EBR_FUNCTION'], type='varchar', alias='ebr_function') }},
        {{ trx_json_extract('src', ['EBR_MEFLAG'], type='varchar', alias='ebr_meflag') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5editionbackreps') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ebr_function', 'ebr_meflag']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ebr_lastsaved']) }}
