{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MAPSCONSENT',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MCT_LASTSAVED'], type='timestamp_ntz', alias='mct_lastsaved') }},
        {{ trx_json_extract('src', ['MCT_USERID'], type='varchar', alias='mct_userid') }},
        {{ trx_json_extract('src', ['MCT_DEVICEID'], type='varchar', alias='mct_deviceid') }},
        {{ trx_json_extract('src', ['MCT_LASTUPDATED'], type='timestamp_ntz', alias='mct_lastupdated') }},
        {{ trx_json_extract('src', ['MCT_UPDATECOUNT'], type='float', alias='mct_updatecount') }},
        {{ trx_json_extract('src', ['MCT_APPID'], type='varchar', alias='mct_appid') }},
        {{ trx_json_extract('src', ['MCT_PRODUCT'], type='varchar', alias='mct_product') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mapsconsent') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mct_appid', 'mct_userid', 'mct_deviceid', 'mct_product']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mct_lastsaved']) }}
