{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DOCSIZES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DSZ_LASTSAVED'], type='timestamp_ntz', alias='dsz_lastsaved') }},
        {{ trx_json_extract('src', ['DSZ_UPDATECOUNT'], type='float', alias='dsz_updatecount') }},
        {{ trx_json_extract('src', ['DSZ_CODE'], type='varchar', alias='dsz_code') }},
        {{ trx_json_extract('src', ['DSZ_DESC'], type='varchar', alias='dsz_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5docsizes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dsz_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dsz_lastsaved']) }}
