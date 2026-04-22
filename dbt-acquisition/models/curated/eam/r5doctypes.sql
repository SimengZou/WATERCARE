{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DOCTYPES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DOT_LASTSAVED'], type='timestamp_ntz', alias='dot_lastsaved') }},
        {{ trx_json_extract('src', ['DOT_EXT'], type='varchar', alias='dot_ext') }},
        {{ trx_json_extract('src', ['DOT_TYPE'], type='varchar', alias='dot_type') }},
        {{ trx_json_extract('src', ['DOT_UPDATECOUNT'], type='float', alias='dot_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5doctypes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dot_ext']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dot_lastsaved']) }}
