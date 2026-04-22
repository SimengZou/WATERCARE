{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USEGRIDSYSDEFAULT',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['USD_LASTSAVED'], type='timestamp_ntz', alias='usd_lastsaved') }},
        {{ trx_json_extract('src', ['USD_USERID'], type='varchar', alias='usd_userid') }},
        {{ trx_json_extract('src', ['USD_DATASPYID'], type='float', alias='usd_dataspyid') }},
        {{ trx_json_extract('src', ['USD_DATASPYFILTER'], type='varchar', alias='usd_dataspyfilter') }},
        {{ trx_json_extract('src', ['USD_GRIDID'], type='float', alias='usd_gridid') }},
        {{ trx_json_extract('src', ['USD_UPDATECOUNT'], type='float', alias='usd_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5usegridsysdefault') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['usd_gridid', 'usd_userid', 'usd_dataspyfilter']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['usd_lastsaved']) }}
