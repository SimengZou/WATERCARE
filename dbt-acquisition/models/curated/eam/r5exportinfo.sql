{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EXPORTINFO',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EPI_LASTSAVED'], type='timestamp_ntz', alias='epi_lastsaved') }},
        {{ trx_json_extract('src', ['EPI_SESSIONID'], type='float', alias='epi_sessionid') }},
        {{ trx_json_extract('src', ['EPI_EXPORT'], type='varchar', alias='epi_export') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5exportinfo') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['epi_lastsaved']) }}
