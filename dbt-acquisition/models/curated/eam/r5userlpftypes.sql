{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERLPFTYPES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LPT_LASTSAVED'], type='timestamp_ntz', alias='lpt_lastsaved') }},
        {{ trx_json_extract('src', ['LPT_TYPE'], type='varchar', alias='lpt_type') }},
        {{ trx_json_extract('src', ['LPT_SEQUENCE'], type='float', alias='lpt_sequence') }},
        {{ trx_json_extract('src', ['LPT_UPDATECOUNT'], type='float', alias='lpt_updatecount') }},
        {{ trx_json_extract('src', ['LPT_LINEARPREFERENCE'], type='varchar', alias='lpt_linearpreference') }},
        {{ trx_json_extract('src', ['LPT_RTYPE'], type='varchar', alias='lpt_rtype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userlpftypes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['lpt_linearpreference', 'lpt_type']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lpt_lastsaved']) }}
