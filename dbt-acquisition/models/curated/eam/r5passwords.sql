{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PASSWORDS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PWD_LASTSAVED'], type='timestamp_ntz', alias='pwd_lastsaved') }},
        {{ trx_json_extract('src', ['PWD_PASSWORD'], type='varchar', alias='pwd_password') }},
        {{ trx_json_extract('src', ['PWD_UPDATECOUNT'], type='float', alias='pwd_updatecount') }},
        {{ trx_json_extract('src', ['PWD_USER'], type='varchar', alias='pwd_user') }},
        {{ trx_json_extract('src', ['PWD_LASTUSED'], type='timestamp_ntz', alias='pwd_lastused') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5passwords') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pwd_user', 'pwd_password', 'pwd_lastused']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pwd_lastsaved']) }}
