{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERFUNCTIONDEFS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UFN_LASTSAVED'], type='timestamp_ntz', alias='ufn_lastsaved') }},
        {{ trx_json_extract('src', ['UFN_UPDATECOUNT'], type='float', alias='ufn_updatecount') }},
        {{ trx_json_extract('src', ['UFN_FUNCTIONNAME'], type='varchar', alias='ufn_functionname') }},
        {{ trx_json_extract('src', ['UFN_DESCRIPTION'], type='varchar', alias='ufn_description') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userfunctiondefs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ufn_functionname']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ufn_lastsaved']) }}
