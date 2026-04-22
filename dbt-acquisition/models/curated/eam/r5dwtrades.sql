{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWTRADES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZTR_LASTSAVED'], type='timestamp_ntz', alias='ztr_lastsaved') }},
        {{ trx_json_extract('src', ['ZTR_CODE'], type='varchar', alias='ztr_code') }},
        {{ trx_json_extract('src', ['ZTR_CLASSDESC'], type='varchar', alias='ztr_classdesc') }},
        {{ trx_json_extract('src', ['ZTR_CLASSCODE'], type='varchar', alias='ztr_classcode') }},
        {{ trx_json_extract('src', ['ZTR_CLASSORG'], type='varchar', alias='ztr_classorg') }},
        {{ trx_json_extract('src', ['ZTR_KEY'], type='float', alias='ztr_key') }},
        {{ trx_json_extract('src', ['ZTR_DESC'], type='varchar', alias='ztr_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwtrades') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ztr_lastsaved']) }}
