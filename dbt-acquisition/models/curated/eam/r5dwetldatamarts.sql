{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWETLDATAMARTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ETL_LASTSAVED'], type='timestamp_ntz', alias='etl_lastsaved') }},
        {{ trx_json_extract('src', ['ETL_DMTTABLE'], type='varchar', alias='etl_dmttable') }},
        {{ trx_json_extract('src', ['ETL_COMDIM'], type='varchar', alias='etl_comdim') }},
        {{ trx_json_extract('src', ['ETL_DIMTABLE'], type='varchar', alias='etl_dimtable') }},
        {{ trx_json_extract('src', ['ETL_MODULE'], type='varchar', alias='etl_module') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwetldatamarts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_lastsaved']) }}
