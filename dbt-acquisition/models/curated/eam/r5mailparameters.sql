{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MAILPARAMETERS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MAP_LASTSAVED'], type='timestamp_ntz', alias='map_lastsaved') }},
        {{ trx_json_extract('src', ['MAP_TEMPLATE'], type='varchar', alias='map_template') }},
        {{ trx_json_extract('src', ['MAP_COLUMN'], type='varchar', alias='map_column') }},
        {{ trx_json_extract('src', ['MAP_UPDATECOUNT'], type='float', alias='map_updatecount') }},
        {{ trx_json_extract('src', ['MAP_ATTRIBPK'], type='varchar', alias='map_attribpk') }},
        {{ trx_json_extract('src', ['MAP_REPORTPARAMETER'], type='float', alias='map_reportparameter') }},
        {{ trx_json_extract('src', ['MAP_TABLE'], type='varchar', alias='map_table') }},
        {{ trx_json_extract('src', ['MAP_PARAMETER'], type='float', alias='map_parameter') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mailparameters') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['map_attribpk', 'map_parameter']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['map_lastsaved']) }}
