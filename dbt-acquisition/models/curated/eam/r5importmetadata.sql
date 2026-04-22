{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IMPORTMETADATA',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IMD_LASTSAVED'], type='timestamp_ntz', alias='imd_lastsaved') }},
        {{ trx_json_extract('src', ['IMD_LINE'], type='float', alias='imd_line') }},
        {{ trx_json_extract('src', ['IMD_METADATA'], type='varchar', alias='imd_metadata') }},
        {{ trx_json_extract('src', ['IMD_SESSIONID'], type='float', alias='imd_sessionid') }},
        {{ trx_json_extract('src', ['IMD_TABLE'], type='varchar', alias='imd_table') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5importmetadata') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['imd_lastsaved']) }}
