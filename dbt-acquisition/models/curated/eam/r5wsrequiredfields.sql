{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSREQUIREDFIELDS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PWR_LASTSAVED'], type='timestamp_ntz', alias='pwr_lastsaved') }},
        {{ trx_json_extract('src', ['PWR_TABNAME'], type='varchar', alias='pwr_tabname') }},
        {{ trx_json_extract('src', ['PWR_REQFIELDS'], type='varchar', alias='pwr_reqfields') }},
        {{ trx_json_extract('src', ['PWR_KEYFIELDS'], type='varchar', alias='pwr_keyfields') }},
        {{ trx_json_extract('src', ['PWR_PAGENAME'], type='varchar', alias='pwr_pagename') }},
        {{ trx_json_extract('src', ['PWR_WEBSERVICE'], type='varchar', alias='pwr_webservice') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wsrequiredfields') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pwr_lastsaved']) }}
