{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERTABCOLUMNS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UTC_LASTSAVED'], type='timestamp_ntz', alias='utc_lastsaved') }},
        {{ trx_json_extract('src', ['UTC_COLUMNNAME'], type='varchar', alias='utc_columnname') }},
        {{ trx_json_extract('src', ['UTC_DATATYPE'], type='varchar', alias='utc_datatype') }},
        {{ trx_json_extract('src', ['UTC_OBJ_XTYPE'], type='varchar', alias='utc_obj_xtype') }},
        {{ trx_json_extract('src', ['UTC_COLLATION'], type='varchar', alias='utc_collation') }},
        {{ trx_json_extract('src', ['UTC_ISID'], type='varchar', alias='utc_isid') }},
        {{ trx_json_extract('src', ['UTC_DATABASE'], type='varchar', alias='utc_database') }},
        {{ trx_json_extract('src', ['UTC_TABLENAME'], type='varchar', alias='utc_tablename') }},
        {{ trx_json_extract('src', ['UTC_COL_XTYPE'], type='float', alias='utc_col_xtype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5usertabcolumns') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['utc_lastsaved']) }}
