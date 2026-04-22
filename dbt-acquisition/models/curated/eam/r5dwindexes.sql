{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWINDEXES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IDX_LASTSAVED'], type='timestamp_ntz', alias='idx_lastsaved') }},
        {{ trx_json_extract('src', ['IDX_TABLE'], type='varchar', alias='idx_table') }},
        {{ trx_json_extract('src', ['IDX_TABLESPACE'], type='varchar', alias='idx_tablespace') }},
        {{ trx_json_extract('src', ['IDX_INDEXTYPE'], type='varchar', alias='idx_indextype') }},
        {{ trx_json_extract('src', ['IDX_INDEXNAME'], type='varchar', alias='idx_indexname') }},
        {{ trx_json_extract('src', ['IDX_FIELDS'], type='varchar', alias='idx_fields') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwindexes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['idx_lastsaved']) }}
