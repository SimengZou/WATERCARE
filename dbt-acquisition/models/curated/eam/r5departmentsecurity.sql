{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DEPARTMENTSECURITY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DSE_LASTSAVED'], type='timestamp_ntz', alias='dse_lastsaved') }},
        {{ trx_json_extract('src', ['DSE_MRC'], type='varchar', alias='dse_mrc') }},
        {{ trx_json_extract('src', ['DSE_READONLY'], type='varchar', alias='dse_readonly') }},
        {{ trx_json_extract('src', ['DSE_UPDATED'], type='timestamp_ntz', alias='dse_updated') }},
        {{ trx_json_extract('src', ['DSE_USER'], type='varchar', alias='dse_user') }},
        {{ trx_json_extract('src', ['DSE_UPDATECOUNT'], type='float', alias='dse_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5departmentsecurity') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dse_user', 'dse_mrc']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dse_lastsaved']) }}
