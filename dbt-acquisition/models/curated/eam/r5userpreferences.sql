{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERPREFERENCES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['USP_LASTSAVED'], type='timestamp_ntz', alias='usp_lastsaved') }},
        {{ trx_json_extract('src', ['USP_TYPE'], type='varchar', alias='usp_type') }},
        {{ trx_json_extract('src', ['USP_VALUE'], type='varchar', alias='usp_value') }},
        {{ trx_json_extract('src', ['USP_UPDATECOUNT'], type='float', alias='usp_updatecount') }},
        {{ trx_json_extract('src', ['USP_USER'], type='varchar', alias='usp_user') }},
        {{ trx_json_extract('src', ['USP_CODE'], type='varchar', alias='usp_code') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userpreferences') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['usp_user', 'usp_type', 'usp_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['usp_lastsaved']) }}
