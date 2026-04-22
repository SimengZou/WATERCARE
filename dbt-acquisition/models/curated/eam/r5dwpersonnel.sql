{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWPERSONNEL',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZPE_LASTSAVED'], type='timestamp_ntz', alias='zpe_lastsaved') }},
        {{ trx_json_extract('src', ['ZPE_CODE'], type='varchar', alias='zpe_code') }},
        {{ trx_json_extract('src', ['ZPE_CLASSDESC'], type='varchar', alias='zpe_classdesc') }},
        {{ trx_json_extract('src', ['ZPE_CLASSCODE'], type='varchar', alias='zpe_classcode') }},
        {{ trx_json_extract('src', ['ZPE_CLASSORG'], type='varchar', alias='zpe_classorg') }},
        {{ trx_json_extract('src', ['ZPE_KEY'], type='float', alias='zpe_key') }},
        {{ trx_json_extract('src', ['ZPE_NAME'], type='varchar', alias='zpe_name') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwpersonnel') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['zpe_lastsaved']) }}
