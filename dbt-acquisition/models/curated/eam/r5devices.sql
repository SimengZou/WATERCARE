{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DEVICES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DEV_LASTSAVED'], type='timestamp_ntz', alias='dev_lastsaved') }},
        {{ trx_json_extract('src', ['DEV_LASTLOGINDATE'], type='timestamp_ntz', alias='dev_lastlogindate') }},
        {{ trx_json_extract('src', ['DEV_CATFLAG'], type='varchar', alias='dev_catflag') }},
        {{ trx_json_extract('src', ['DEV_RTYPE'], type='varchar', alias='dev_rtype') }},
        {{ trx_json_extract('src', ['DEV_TYPE'], type='varchar', alias='dev_type') }},
        {{ trx_json_extract('src', ['DEV_CATEGORY'], type='varchar', alias='dev_category') }},
        {{ trx_json_extract('src', ['DEV_DRIVER'], type='varchar', alias='dev_driver') }},
        {{ trx_json_extract('src', ['DEV_MODE'], type='varchar', alias='dev_mode') }},
        {{ trx_json_extract('src', ['DEV_SPECIAL'], type='varchar', alias='dev_special') }},
        {{ trx_json_extract('src', ['DEV_ORIENTATION'], type='varchar', alias='dev_orientation') }},
        {{ trx_json_extract('src', ['DEV_DESTINATION'], type='varchar', alias='dev_destination') }},
        {{ trx_json_extract('src', ['DEV_ORG'], type='varchar', alias='dev_org') }},
        {{ trx_json_extract('src', ['DEV_UPDATECOUNT'], type='float', alias='dev_updatecount') }},
        {{ trx_json_extract('src', ['DEV_UPDATED'], type='timestamp_ntz', alias='dev_updated') }},
        {{ trx_json_extract('src', ['DEV_NOTUSED'], type='varchar', alias='dev_notused') }},
        {{ trx_json_extract('src', ['DEV_CODE'], type='varchar', alias='dev_code') }},
        {{ trx_json_extract('src', ['DEV_DESC'], type='varchar', alias='dev_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5devices') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dev_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dev_lastsaved']) }}
