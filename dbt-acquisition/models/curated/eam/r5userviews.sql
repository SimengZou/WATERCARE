{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERVIEWS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UVW_LASTSAVED'], type='timestamp_ntz', alias='uvw_lastsaved') }},
        {{ trx_json_extract('src', ['UVW_DESC'], type='varchar', alias='uvw_desc') }},
        {{ trx_json_extract('src', ['UVW_NAME'], type='varchar', alias='uvw_name') }},
        {{ trx_json_extract('src', ['UVW_STMT'], type='varchar', alias='uvw_stmt') }},
        {{ trx_json_extract('src', ['UVW_ACTIVE'], type='varchar', alias='uvw_active') }},
        {{ trx_json_extract('src', ['UVW_UPDATECOUNT'], type='float', alias='uvw_updatecount') }},
        {{ trx_json_extract('src', ['UVW_CODE'], type='varchar', alias='uvw_code') }},
        {{ trx_json_extract('src', ['UVW_NOTE'], type='varchar', alias='uvw_note') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userviews') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['uvw_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['uvw_lastsaved']) }}
