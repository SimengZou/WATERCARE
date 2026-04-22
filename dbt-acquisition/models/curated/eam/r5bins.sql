{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BINS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['BIN_LASTSAVED'], type='timestamp_ntz', alias='bin_lastsaved') }},
        {{ trx_json_extract('src', ['BIN_CODE'], type='varchar', alias='bin_code') }},
        {{ trx_json_extract('src', ['BIN_DESC'], type='varchar', alias='bin_desc') }},
        {{ trx_json_extract('src', ['BIN_CREATED'], type='timestamp_ntz', alias='bin_created') }},
        {{ trx_json_extract('src', ['BIN_CREATEDBY'], type='varchar', alias='bin_createdby') }},
        {{ trx_json_extract('src', ['BIN_UPDATEDBY'], type='varchar', alias='bin_updatedby') }},
        {{ trx_json_extract('src', ['BIN_NOTUSED'], type='varchar', alias='bin_notused') }},
        {{ trx_json_extract('src', ['BIN_UPDATECOUNT'], type='float', alias='bin_updatecount') }},
        {{ trx_json_extract('src', ['BIN_FORSTOCK'], type='varchar', alias='bin_forstock') }},
        {{ trx_json_extract('src', ['BIN_FORHELDITEMS'], type='varchar', alias='bin_forhelditems') }},
        {{ trx_json_extract('src', ['BIN_STORE'], type='varchar', alias='bin_store') }},
        {{ trx_json_extract('src', ['BIN_UPDATED'], type='timestamp_ntz', alias='bin_updated') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5bins') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['bin_store', 'bin_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['bin_lastsaved']) }}
