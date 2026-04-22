{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MAILURLMAPPING',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MUM_LASTSAVED'], type='timestamp_ntz', alias='mum_lastsaved') }},
        {{ trx_json_extract('src', ['MUM_FUNCTION'], type='varchar', alias='mum_function') }},
        {{ trx_json_extract('src', ['MUM_TABLECOLUMN'], type='varchar', alias='mum_tablecolumn') }},
        {{ trx_json_extract('src', ['MUM_TABLE'], type='varchar', alias='mum_table') }},
        {{ trx_json_extract('src', ['MUM_JSPFIELD'], type='varchar', alias='mum_jspfield') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mailurlmapping') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mum_lastsaved']) }}
