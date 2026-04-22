{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5LANGUAGES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LAN_LASTSAVED'], type='timestamp_ntz', alias='lan_lastsaved') }},
        {{ trx_json_extract('src', ['LAN_CLASS'], type='varchar', alias='lan_class') }},
        {{ trx_json_extract('src', ['LAN_TXTTYPE'], type='varchar', alias='lan_txttype') }},
        {{ trx_json_extract('src', ['LAN_CLASS_ORG'], type='varchar', alias='lan_class_org') }},
        {{ trx_json_extract('src', ['LAN_UPDATECOUNT'], type='float', alias='lan_updatecount') }},
        {{ trx_json_extract('src', ['LAN_RSTATUS'], type='varchar', alias='lan_rstatus') }},
        {{ trx_json_extract('src', ['LAN_LASTCREATED'], type='timestamp_ntz', alias='lan_lastcreated') }},
        {{ trx_json_extract('src', ['LAN_PROCESSSTART'], type='timestamp_ntz', alias='lan_processstart') }},
        {{ trx_json_extract('src', ['LAN_PROCESSEND'], type='timestamp_ntz', alias='lan_processend') }},
        {{ trx_json_extract('src', ['LAN_INSTALLED'], type='varchar', alias='lan_installed') }},
        {{ trx_json_extract('src', ['LAN_NOTUSED'], type='varchar', alias='lan_notused') }},
        {{ trx_json_extract('src', ['LAN_AVAILABLE'], type='varchar', alias='lan_available') }},
        {{ trx_json_extract('src', ['LAN_ERRORMESSAGE'], type='varchar', alias='lan_errormessage') }},
        {{ trx_json_extract('src', ['LAN_CODE'], type='varchar', alias='lan_code') }},
        {{ trx_json_extract('src', ['LAN_DESC'], type='varchar', alias='lan_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5languages') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['lan_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lan_lastsaved']) }}
