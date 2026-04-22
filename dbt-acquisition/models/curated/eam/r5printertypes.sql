{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PRINTERTYPES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PNT_LASTSAVED'], type='timestamp_ntz', alias='pnt_lastsaved') }},
        {{ trx_json_extract('src', ['PNT_UPDATECOUNT'], type='float', alias='pnt_updatecount') }},
        {{ trx_json_extract('src', ['PNT_CODE'], type='varchar', alias='pnt_code') }},
        {{ trx_json_extract('src', ['PNT_DESC'], type='varchar', alias='pnt_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5printertypes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pnt_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pnt_lastsaved']) }}
