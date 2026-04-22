{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DDFIELD',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DDF_LASTSAVED'], type='timestamp_ntz', alias='ddf_lastsaved') }},
        {{ trx_json_extract('src', ['DDF_SOURCENAME'], type='varchar', alias='ddf_sourcename') }},
        {{ trx_json_extract('src', ['DDF_DATATYPE'], type='varchar', alias='ddf_datatype') }},
        {{ trx_json_extract('src', ['DDF_DESC'], type='varchar', alias='ddf_desc') }},
        {{ trx_json_extract('src', ['DDF_UPDATECOUNT'], type='float', alias='ddf_updatecount') }},
        {{ trx_json_extract('src', ['DDF_RENTITY'], type='varchar', alias='ddf_rentity') }},
        {{ trx_json_extract('src', ['DDF_LVGRID'], type='varchar', alias='ddf_lvgrid') }},
        {{ trx_json_extract('src', ['DDF_TABLENAME'], type='varchar', alias='ddf_tablename') }},
        {{ trx_json_extract('src', ['DDF_FIELDID'], type='float', alias='ddf_fieldid') }},
        {{ trx_json_extract('src', ['DDF_VALUEMAPID'], type='float', alias='ddf_valuemapid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5ddfield') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ddf_fieldid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ddf_lastsaved']) }}
