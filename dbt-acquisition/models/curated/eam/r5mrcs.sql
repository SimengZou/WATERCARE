{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MRCS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MRC_LASTSAVED'], type='timestamp_ntz', alias='mrc_lastsaved') }},
        {{ trx_json_extract('src', ['MRC_AVAILABLEFORCUS'], type='varchar', alias='mrc_availableforcus') }},
        {{ trx_json_extract('src', ['MRC_CLASS'], type='varchar', alias='mrc_class') }},
        {{ trx_json_extract('src', ['MRC_STORE'], type='varchar', alias='mrc_store') }},
        {{ trx_json_extract('src', ['MRC_DFLTSCREENER'], type='varchar', alias='mrc_dfltscreener') }},
        {{ trx_json_extract('src', ['MRC_SCHEDGROUP'], type='varchar', alias='mrc_schedgroup') }},
        {{ trx_json_extract('src', ['MRC_CAPACITY'], type='float', alias='mrc_capacity') }},
        {{ trx_json_extract('src', ['MRC_ORG'], type='varchar', alias='mrc_org') }},
        {{ trx_json_extract('src', ['MRC_CLASS_ORG'], type='varchar', alias='mrc_class_org') }},
        {{ trx_json_extract('src', ['MRC_UPDATECOUNT'], type='float', alias='mrc_updatecount') }},
        {{ trx_json_extract('src', ['MRC_CREATED'], type='timestamp_ntz', alias='mrc_created') }},
        {{ trx_json_extract('src', ['MRC_UPDATED'], type='timestamp_ntz', alias='mrc_updated') }},
        {{ trx_json_extract('src', ['MRC_NOTUSED'], type='varchar', alias='mrc_notused') }},
        {{ trx_json_extract('src', ['MRC_SEGMENTVALUE'], type='varchar', alias='mrc_segmentvalue') }},
        {{ trx_json_extract('src', ['MRC_DEPOT'], type='varchar', alias='mrc_depot') }},
        {{ trx_json_extract('src', ['MRC_DEPOT_ORG'], type='varchar', alias='mrc_depot_org') }},
        {{ trx_json_extract('src', ['MRC_CODE'], type='varchar', alias='mrc_code') }},
        {{ trx_json_extract('src', ['MRC_DESC'], type='varchar', alias='mrc_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mrcs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mrc_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mrc_lastsaved']) }}
