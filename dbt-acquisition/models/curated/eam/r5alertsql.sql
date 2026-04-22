{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTSQL',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ALS_LASTSAVED'], type='timestamp_ntz', alias='als_lastsaved') }},
        {{ trx_json_extract('src', ['ALS_RTYPE'], type='varchar', alias='als_rtype') }},
        {{ trx_json_extract('src', ['ALS_STMT'], type='varchar', alias='als_stmt') }},
        {{ trx_json_extract('src', ['ALS_ABORTONFAILURE'], type='varchar', alias='als_abortonfailure') }},
        {{ trx_json_extract('src', ['ALS_ACTIVE'], type='varchar', alias='als_active') }},
        {{ trx_json_extract('src', ['ALS_COMMENT'], type='varchar', alias='als_comment') }},
        {{ trx_json_extract('src', ['ALS_UPDATECOUNT'], type='float', alias='als_updatecount') }},
        {{ trx_json_extract('src', ['ALS_ALERT'], type='varchar', alias='als_alert') }},
        {{ trx_json_extract('src', ['ALS_INCLUDEPREVIEW'], type='varchar', alias='als_includepreview') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alertsql') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['als_alert', 'als_rtype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['als_lastsaved']) }}
