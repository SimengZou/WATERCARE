{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SHFPERS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SHP_LASTSAVED'], type='timestamp_ntz', alias='shp_lastsaved') }},
        {{ trx_json_extract('src', ['SHP_PERSON'], type='varchar', alias='shp_person') }},
        {{ trx_json_extract('src', ['SHP_END'], type='timestamp_ntz', alias='shp_end') }},
        {{ trx_json_extract('src', ['SHP_UPDATECOUNT'], type='float', alias='shp_updatecount') }},
        {{ trx_json_extract('src', ['SHP_SHIFT'], type='varchar', alias='shp_shift') }},
        {{ trx_json_extract('src', ['SHP_START'], type='timestamp_ntz', alias='shp_start') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5shfpers') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['shp_shift', 'shp_person', 'shp_start', 'shp_end']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['shp_lastsaved']) }}
