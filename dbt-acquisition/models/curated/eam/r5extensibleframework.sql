{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EXTENSIBLEFRAMEWORK',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EFR_LASTSAVED'], type='timestamp_ntz', alias='efr_lastsaved') }},
        {{ trx_json_extract('src', ['EFR_SOURCECODE'], type='varchar', alias='efr_sourcecode') }},
        {{ trx_json_extract('src', ['EFR_USERFUNCTION'], type='varchar', alias='efr_userfunction') }},
        {{ trx_json_extract('src', ['EFR_UPDATECOUNT'], type='float', alias='efr_updatecount') }},
        {{ trx_json_extract('src', ['EFR_NAME'], type='varchar', alias='efr_name') }},
        {{ trx_json_extract('src', ['EFR_ACTIVE'], type='varchar', alias='efr_active') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5extensibleframework') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['efr_name']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['efr_lastsaved']) }}
