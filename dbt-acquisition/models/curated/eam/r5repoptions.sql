{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REPOPTIONS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ROP_LASTSAVED'], type='timestamp_ntz', alias='rop_lastsaved') }},
        {{ trx_json_extract('src', ['ROP_PARAMETER'], type='varchar', alias='rop_parameter') }},
        {{ trx_json_extract('src', ['ROP_SEQNO'], type='float', alias='rop_seqno') }},
        {{ trx_json_extract('src', ['ROP_UPDATECOUNT'], type='float', alias='rop_updatecount') }},
        {{ trx_json_extract('src', ['ROP_UPDATED'], type='timestamp_ntz', alias='rop_updated') }},
        {{ trx_json_extract('src', ['ROP_MEKEY'], type='varchar', alias='rop_mekey') }},
        {{ trx_json_extract('src', ['ROP_FUNCTION'], type='varchar', alias='rop_function') }},
        {{ trx_json_extract('src', ['ROP_VALUE'], type='varchar', alias='rop_value') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5repoptions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rop_function', 'rop_parameter', 'rop_seqno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rop_lastsaved']) }}
