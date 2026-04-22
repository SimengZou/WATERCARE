{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5LANGINST',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LIN_LASTSAVED'], type='timestamp_ntz', alias='lin_lastsaved') }},
        {{ trx_json_extract('src', ['LIN_TXTTYPE'], type='varchar', alias='lin_txttype') }},
        {{ trx_json_extract('src', ['LIN_LANGFILE'], type='varchar', alias='lin_langfile') }},
        {{ trx_json_extract('src', ['LIN_CODE'], type='varchar', alias='lin_code') }},
        {{ trx_json_extract('src', ['LIN_UPDATECOUNT'], type='float', alias='lin_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5langinst') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['lin_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lin_lastsaved']) }}
