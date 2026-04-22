{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SCREENCACHE',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SNC_LASTSAVED'], type='timestamp_ntz', alias='snc_lastsaved') }},
        {{ trx_json_extract('src', ['SNC_UPDATECOUNT'], type='float', alias='snc_updatecount') }},
        {{ trx_json_extract('src', ['SNC_USER'], type='varchar', alias='snc_user') }},
        {{ trx_json_extract('src', ['SNC_FUNCTION'], type='varchar', alias='snc_function') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5screencache') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['snc_user', 'snc_function']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['snc_lastsaved']) }}
