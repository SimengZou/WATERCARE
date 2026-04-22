{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MOBILELISTENTITIES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LET_LASTSAVED'], type='timestamp_ntz', alias='let_lastsaved') }},
        {{ trx_json_extract('src', ['LET_MASK'], type='float', alias='let_mask') }},
        {{ trx_json_extract('src', ['LET_GROUPID'], type='float', alias='let_groupid') }},
        {{ trx_json_extract('src', ['LET_BLOCKSIZE'], type='float', alias='let_blocksize') }},
        {{ trx_json_extract('src', ['LET_DELINSUPD'], type='float', alias='let_delinsupd') }},
        {{ trx_json_extract('src', ['LET_FILTER'], type='varchar', alias='let_filter') }},
        {{ trx_json_extract('src', ['LET_TABLENAME'], type='varchar', alias='let_tablename') }},
        {{ trx_json_extract('src', ['LET_DOWNCAT'], type='varchar', alias='let_downcat') }},
        {{ trx_json_extract('src', ['LET_ORGKEY'], type='varchar', alias='let_orgkey') }},
        {{ trx_json_extract('src', ['LET_DOWNGROUP'], type='float', alias='let_downgroup') }},
        {{ trx_json_extract('src', ['LET_UPDATECOUNT'], type='float', alias='let_updatecount') }},
        {{ trx_json_extract('src', ['LET_UPDATED'], type='timestamp_ntz', alias='let_updated') }},
        {{ trx_json_extract('src', ['LET_TABLEUPDATED'], type='timestamp_ntz', alias='let_tableupdated') }},
        {{ trx_json_extract('src', ['LET_FILTERPARAMS'], type='varchar', alias='let_filterparams') }},
        {{ trx_json_extract('src', ['LET_MASTERTABLE'], type='varchar', alias='let_mastertable') }},
        {{ trx_json_extract('src', ['LET_CACHEDATA'], type='varchar', alias='let_cachedata') }},
        {{ trx_json_extract('src', ['LET_RENTITIES'], type='varchar', alias='let_rentities') }},
        {{ trx_json_extract('src', ['LET_DDSPYID'], type='float', alias='let_ddspyid') }},
        {{ trx_json_extract('src', ['LET_DATAGRID'], type='varchar', alias='let_datagrid') }},
        {{ trx_json_extract('src', ['LET_SEQNO'], type='float', alias='let_seqno') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mobilelistentities') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['let_lastsaved']) }}
