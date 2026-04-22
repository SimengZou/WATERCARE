{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MOBILEDBSYNC',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MDS_LASTSAVED'], type='timestamp_ntz', alias='mds_lastsaved') }},
        {{ trx_json_extract('src', ['MDS_USER'], type='varchar', alias='mds_user') }},
        {{ trx_json_extract('src', ['MDS_ORG'], type='varchar', alias='mds_org') }},
        {{ trx_json_extract('src', ['MDS_STATUS'], type='varchar', alias='mds_status') }},
        {{ trx_json_extract('src', ['MDS_DBLASTUPDATEDDATE'], type='timestamp_ntz', alias='mds_dblastupdateddate') }},
        {{ trx_json_extract('src', ['MDS_STATUS_MSG'], type='varchar', alias='mds_status_msg') }},
        {{ trx_json_extract('src', ['MDS_SYNC_REQUEST'], type='varchar', alias='mds_sync_request') }},
        {{ trx_json_extract('src', ['MDS_DOWNLOAD'], type='varchar', alias='mds_download') }},
        {{ trx_json_extract('src', ['MDS_FILENAME'], type='varchar', alias='mds_filename') }},
        {{ trx_json_extract('src', ['MDS_UPDATECOUNT'], type='float', alias='mds_updatecount') }},
        {{ trx_json_extract('src', ['MDS_SYNCID'], type='float', alias='mds_syncid') }},
        {{ trx_json_extract('src', ['MDS_GRIDS_PROCESSED'], type='float', alias='mds_grids_processed') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mobiledbsync') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mds_syncid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mds_lastsaved']) }}
