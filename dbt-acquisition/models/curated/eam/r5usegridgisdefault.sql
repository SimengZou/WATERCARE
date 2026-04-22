{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USEGRIDGISDEFAULT',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UGD_LASTSAVED'], type='timestamp_ntz', alias='ugd_lastsaved') }},
        {{ trx_json_extract('src', ['UGD_USERID'], type='varchar', alias='ugd_userid') }},
        {{ trx_json_extract('src', ['UGD_GISSERVICE'], type='varchar', alias='ugd_gisservice') }},
        {{ trx_json_extract('src', ['UGD_DATASPYID'], type='float', alias='ugd_dataspyid') }},
        {{ trx_json_extract('src', ['UGD_UPDATECOUNT'], type='float', alias='ugd_updatecount') }},
        {{ trx_json_extract('src', ['UGD_GRIDID'], type='float', alias='ugd_gridid') }},
        {{ trx_json_extract('src', ['UGD_GISLAYER'], type='varchar', alias='ugd_gislayer') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5usegridgisdefault') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ugd_gridid', 'ugd_userid', 'ugd_gisservice', 'ugd_gislayer']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ugd_lastsaved']) }}
