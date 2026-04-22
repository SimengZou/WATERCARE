{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld172 Mappings by Taxonomy Account table, Generated 2026-04-10T19:41:43Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['taxo'], type='varchar', alias='taxo') }},
        {{ trx_json_extract('src', ['vers'], type='int', alias='vers') }},
        {{ trx_json_extract('src', ['acid'], type='varchar', alias='acid') }},
        {{ trx_json_extract('src', ['line'], type='int', alias='line') }},
        {{ trx_json_extract('src', ['fcom'], type='int', alias='fcom') }},
        {{ trx_json_extract('src', ['tcom'], type='int', alias='tcom') }},
        {{ trx_json_extract('src', ['flac'], type='varchar', alias='flac') }},
        {{ trx_json_extract('src', ['tlac'], type='varchar', alias='tlac') }},
        {{ trx_json_extract('src', ['fdim_1'], type='varchar', alias='fdim_1') }},
        {{ trx_json_extract('src', ['fdim_2'], type='varchar', alias='fdim_2') }},
        {{ trx_json_extract('src', ['fdim_3'], type='varchar', alias='fdim_3') }},
        {{ trx_json_extract('src', ['fdim_4'], type='varchar', alias='fdim_4') }},
        {{ trx_json_extract('src', ['fdim_5'], type='varchar', alias='fdim_5') }},
        {{ trx_json_extract('src', ['fdim_6'], type='varchar', alias='fdim_6') }},
        {{ trx_json_extract('src', ['fdim_7'], type='varchar', alias='fdim_7') }},
        {{ trx_json_extract('src', ['fdim_8'], type='varchar', alias='fdim_8') }},
        {{ trx_json_extract('src', ['fdim_9'], type='varchar', alias='fdim_9') }},
        {{ trx_json_extract('src', ['fdim_10'], type='varchar', alias='fdim_10') }},
        {{ trx_json_extract('src', ['fdim_11'], type='varchar', alias='fdim_11') }},
        {{ trx_json_extract('src', ['fdim_12'], type='varchar', alias='fdim_12') }},
        {{ trx_json_extract('src', ['tdim_1'], type='varchar', alias='tdim_1') }},
        {{ trx_json_extract('src', ['tdim_2'], type='varchar', alias='tdim_2') }},
        {{ trx_json_extract('src', ['tdim_3'], type='varchar', alias='tdim_3') }},
        {{ trx_json_extract('src', ['tdim_4'], type='varchar', alias='tdim_4') }},
        {{ trx_json_extract('src', ['tdim_5'], type='varchar', alias='tdim_5') }},
        {{ trx_json_extract('src', ['tdim_6'], type='varchar', alias='tdim_6') }},
        {{ trx_json_extract('src', ['tdim_7'], type='varchar', alias='tdim_7') }},
        {{ trx_json_extract('src', ['tdim_8'], type='varchar', alias='tdim_8') }},
        {{ trx_json_extract('src', ['tdim_9'], type='varchar', alias='tdim_9') }},
        {{ trx_json_extract('src', ['tdim_10'], type='varchar', alias='tdim_10') }},
        {{ trx_json_extract('src', ['tdim_11'], type='varchar', alias='tdim_11') }},
        {{ trx_json_extract('src', ['tdim_12'], type='varchar', alias='tdim_12') }},
        {{ trx_json_extract('src', ['taxo_vers_acid_ref_compnr'], type='int', alias='taxo_vers_acid_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld172') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'taxo', 'vers', 'acid', 'line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
