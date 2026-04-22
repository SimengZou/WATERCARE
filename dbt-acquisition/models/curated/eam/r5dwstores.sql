{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWSTORES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZSO_LASTSAVED'], type='timestamp_ntz', alias='zso_lastsaved') }},
        {{ trx_json_extract('src', ['ZSO_CODE'], type='varchar', alias='zso_code') }},
        {{ trx_json_extract('src', ['ZSO_DESC'], type='varchar', alias='zso_desc') }},
        {{ trx_json_extract('src', ['ZSO_LOCATIONDESC'], type='varchar', alias='zso_locationdesc') }},
        {{ trx_json_extract('src', ['ZSO_CLASSORG'], type='varchar', alias='zso_classorg') }},
        {{ trx_json_extract('src', ['ZSO_CLASSDESC'], type='varchar', alias='zso_classdesc') }},
        {{ trx_json_extract('src', ['ZSO_LOCATIONCODE'], type='varchar', alias='zso_locationcode') }},
        {{ trx_json_extract('src', ['ZSO_LOCATIONORG'], type='varchar', alias='zso_locationorg') }},
        {{ trx_json_extract('src', ['ZSO_KEY'], type='float', alias='zso_key') }},
        {{ trx_json_extract('src', ['ZSO_CLASSCODE'], type='varchar', alias='zso_classcode') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwstores') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['zso_lastsaved']) }}
