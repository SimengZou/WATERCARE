{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MOBILEPNREGISTRY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MPN_LASTSAVED'], type='timestamp_ntz', alias='mpn_lastsaved') }},
        {{ trx_json_extract('src', ['MPN_DEVICEID'], type='varchar', alias='mpn_deviceid') }},
        {{ trx_json_extract('src', ['MPN_PLATFORM'], type='varchar', alias='mpn_platform') }},
        {{ trx_json_extract('src', ['MPN_TOKEN'], type='varchar', alias='mpn_token') }},
        {{ trx_json_extract('src', ['MPN_USER'], type='varchar', alias='mpn_user') }},
        {{ trx_json_extract('src', ['MPN_CREATEDBY'], type='varchar', alias='mpn_createdby') }},
        {{ trx_json_extract('src', ['MPN_UPDATED'], type='timestamp_ntz', alias='mpn_updated') }},
        {{ trx_json_extract('src', ['MPN_UPDATEDBY'], type='varchar', alias='mpn_updatedby') }},
        {{ trx_json_extract('src', ['MPN_UPDATECOUNT'], type='float', alias='mpn_updatecount') }},
        {{ trx_json_extract('src', ['MPN_APPID'], type='varchar', alias='mpn_appid') }},
        {{ trx_json_extract('src', ['MPN_CREATED'], type='timestamp_ntz', alias='mpn_created') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mobilepnregistry') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mpn_appid', 'mpn_deviceid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mpn_lastsaved']) }}
