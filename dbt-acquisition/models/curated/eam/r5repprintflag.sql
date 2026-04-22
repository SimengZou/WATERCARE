{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REPPRINTFLAG',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RPF_LASTSAVED'], type='timestamp_ntz', alias='rpf_lastsaved') }},
        {{ trx_json_extract('src', ['RPF_UPDATECOUNT'], type='float', alias='rpf_updatecount') }},
        {{ trx_json_extract('src', ['RPF_DESTYPE'], type='varchar', alias='rpf_destype') }},
        {{ trx_json_extract('src', ['RPF_VALUE'], type='varchar', alias='rpf_value') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5repprintflag') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rpf_lastsaved']) }}
