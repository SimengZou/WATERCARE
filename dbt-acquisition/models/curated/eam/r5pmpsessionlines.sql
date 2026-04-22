{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PMPSESSIONLINES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PPL_LASTSAVED'], type='timestamp_ntz', alias='ppl_lastsaved') }},
        {{ trx_json_extract('src', ['PPL_OBJECT'], type='varchar', alias='ppl_object') }},
        {{ trx_json_extract('src', ['PPL_OBJORG'], type='varchar', alias='ppl_objorg') }},
        {{ trx_json_extract('src', ['PPL_PPM'], type='varchar', alias='ppl_ppm') }},
        {{ trx_json_extract('src', ['PPL_PPOPK'], type='float', alias='ppl_ppopk') }},
        {{ trx_json_extract('src', ['PPL_SESSIONID'], type='float', alias='ppl_sessionid') }},
        {{ trx_json_extract('src', ['PPL_UPDATECOUNT'], type='float', alias='ppl_updatecount') }},
        {{ trx_json_extract('src', ['PPL_LINE'], type='float', alias='ppl_line') }},
        {{ trx_json_extract('src', ['PPL_NESTINGREF'], type='varchar', alias='ppl_nestingref') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pmpsessionlines') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ppl_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ppl_lastsaved']) }}
