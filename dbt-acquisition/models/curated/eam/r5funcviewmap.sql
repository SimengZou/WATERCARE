{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FUNCVIEWMAP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FVM_LASTSAVED'], type='timestamp_ntz', alias='fvm_lastsaved') }},
        {{ trx_json_extract('src', ['FVM_FUNCTION'], type='varchar', alias='fvm_function') }},
        {{ trx_json_extract('src', ['FVM_FUNCVIEW'], type='varchar', alias='fvm_funcview') }},
        {{ trx_json_extract('src', ['FVM_GROUP'], type='varchar', alias='fvm_group') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5funcviewmap') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fvm_funcview', 'fvm_group']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fvm_lastsaved']) }}
