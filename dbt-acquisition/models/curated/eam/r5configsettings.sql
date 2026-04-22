{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CONFIGSETTINGS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CFS_LASTSAVED'], type='timestamp_ntz', alias='cfs_lastsaved') }},
        {{ trx_json_extract('src', ['CFS_GROUP'], type='varchar', alias='cfs_group') }},
        {{ trx_json_extract('src', ['CFS_USER'], type='varchar', alias='cfs_user') }},
        {{ trx_json_extract('src', ['CFS_COMPTYPE'], type='varchar', alias='cfs_comptype') }},
        {{ trx_json_extract('src', ['CFS_CONFIG'], type='float', alias='cfs_config') }},
        {{ trx_json_extract('src', ['CFS_CREATED'], type='timestamp_ntz', alias='cfs_created') }},
        {{ trx_json_extract('src', ['CFS_UPDATED'], type='timestamp_ntz', alias='cfs_updated') }},
        {{ trx_json_extract('src', ['CFS_UPDATECOUNT'], type='float', alias='cfs_updatecount') }},
        {{ trx_json_extract('src', ['CFS_DESK'], type='varchar', alias='cfs_desk') }},
        {{ trx_json_extract('src', ['CFS_CODE'], type='varchar', alias='cfs_code') }},
        {{ trx_json_extract('src', ['CFS_XMLCONFIG'], type='varchar', alias='cfs_xmlconfig') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5configsettings') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cfs_code', 'cfs_group', 'cfs_user', 'cfs_comptype', 'cfs_config', 'cfs_desk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cfs_lastsaved']) }}
