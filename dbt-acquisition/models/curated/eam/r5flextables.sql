{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FLEXTABLES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FLT_LASTSAVED'], type='timestamp_ntz', alias='flt_lastsaved') }},
        {{ trx_json_extract('src', ['FLT_TABLE'], type='varchar', alias='flt_table') }},
        {{ trx_json_extract('src', ['FLT_UPDATECOUNT'], type='float', alias='flt_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5flextables') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['flt_table']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['flt_lastsaved']) }}
