{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm111 Fixed Price Terms table, Generated 2026-04-10T19:42:30Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['term'], type='int', alias='term') }},
        {{ trx_json_extract('src', ['cfln'], type='int', alias='cfln') }},
        {{ trx_json_extract('src', ['cseq'], type='int', alias='cseq') }},
        {{ trx_json_extract('src', ['fplv'], type='int', alias='fplv') }},
        {{ trx_json_extract('src', ['fplv_kw'], type='varchar', alias='fplv_kw') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['camr'], type='varchar', alias='camr') }},
        {{ trx_json_extract('src', ['caro'], type='varchar', alias='caro') }},
        {{ trx_json_extract('src', ['pris'], type='float', alias='pris') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['term_cfln_ref_compnr'], type='int', alias='term_cfln_ref_compnr') }},
        {{ trx_json_extract('src', ['cact_ref_compnr'], type='int', alias='cact_ref_compnr') }},
        {{ trx_json_extract('src', ['camr_ref_compnr'], type='int', alias='camr_ref_compnr') }},
        {{ trx_json_extract('src', ['caro_ref_compnr'], type='int', alias='caro_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['pris_homc'], type='float', alias='pris_homc') }},
        {{ trx_json_extract('src', ['pris_rpc1'], type='float', alias='pris_rpc1') }},
        {{ trx_json_extract('src', ['pris_rpc2'], type='float', alias='pris_rpc2') }},
        {{ trx_json_extract('src', ['pris_refc'], type='float', alias='pris_refc') }},
        {{ trx_json_extract('src', ['pris_dwhc'], type='float', alias='pris_dwhc') }},
        {{ trx_json_extract('src', ['camt_cntc'], type='float', alias='camt_cntc') }},
        {{ trx_json_extract('src', ['camt_homc'], type='float', alias='camt_homc') }},
        {{ trx_json_extract('src', ['camt_rpc1'], type='float', alias='camt_rpc1') }},
        {{ trx_json_extract('src', ['camt_rpc2'], type='float', alias='camt_rpc2') }},
        {{ trx_json_extract('src', ['camt_refc'], type='float', alias='camt_refc') }},
        {{ trx_json_extract('src', ['camt_dwhc'], type='float', alias='camt_dwhc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm111') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'term', 'cfln', 'cseq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
