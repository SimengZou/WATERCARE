{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWMRCS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZMR_LASTSAVED'], type='timestamp_ntz', alias='zmr_lastsaved') }},
        {{ trx_json_extract('src', ['ZMR_CODE'], type='varchar', alias='zmr_code') }},
        {{ trx_json_extract('src', ['ZMR_CLASSDESC'], type='varchar', alias='zmr_classdesc') }},
        {{ trx_json_extract('src', ['ZMR_CLASSCODE'], type='varchar', alias='zmr_classcode') }},
        {{ trx_json_extract('src', ['ZMR_CLASSORG'], type='varchar', alias='zmr_classorg') }},
        {{ trx_json_extract('src', ['ZMR_KEY'], type='float', alias='zmr_key') }},
        {{ trx_json_extract('src', ['ZMR_DESC'], type='varchar', alias='zmr_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwmrcs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['zmr_lastsaved']) }}
