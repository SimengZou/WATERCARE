{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm139 Uptime Terms table, Generated 2026-04-10T19:42:32Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['term'], type='int', alias='term') }},
        {{ trx_json_extract('src', ['cfln'], type='int', alias='cfln') }},
        {{ trx_json_extract('src', ['cctp'], type='varchar', alias='cctp') }},
        {{ trx_json_extract('src', ['cotp'], type='int', alias='cotp') }},
        {{ trx_json_extract('src', ['cotp_kw'], type='varchar', alias='cotp_kw') }},
        {{ trx_json_extract('src', ['nrbt'], type='int', alias='nrbt') }},
        {{ trx_json_extract('src', ['cseq'], type='int', alias='cseq') }},
        {{ trx_json_extract('src', ['avtm'], type='float', alias='avtm') }},
        {{ trx_json_extract('src', ['utpc'], type='float', alias='utpc') }},
        {{ trx_json_extract('src', ['cstm'], type='float', alias='cstm') }},
        {{ trx_json_extract('src', ['sptm'], type='float', alias='sptm') }},
        {{ trx_json_extract('src', ['crmt'], type='float', alias='crmt') }},
        {{ trx_json_extract('src', ['proc'], type='int', alias='proc') }},
        {{ trx_json_extract('src', ['proc_kw'], type='varchar', alias='proc_kw') }},
        {{ trx_json_extract('src', ['camt_1'], type='float', alias='camt_1') }},
        {{ trx_json_extract('src', ['camt_2'], type='float', alias='camt_2') }},
        {{ trx_json_extract('src', ['camt_3'], type='float', alias='camt_3') }},
        {{ trx_json_extract('src', ['amnt'], type='float', alias='amnt') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['term_cfln_cctp_cotp_nrbt_ref_compnr'], type='int', alias='term_cfln_cctp_cotp_nrbt_ref_compnr') }},
        {{ trx_json_extract('src', ['term_cfln_ref_compnr'], type='int', alias='term_cfln_ref_compnr') }},
        {{ trx_json_extract('src', ['cctp_ref_compnr'], type='int', alias='cctp_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['camt_cntc'], type='float', alias='camt_cntc') }},
        {{ trx_json_extract('src', ['camt_refc'], type='float', alias='camt_refc') }},
        {{ trx_json_extract('src', ['camt_dwhc'], type='float', alias='camt_dwhc') }},
        {{ trx_json_extract('src', ['amnt_homc'], type='float', alias='amnt_homc') }},
        {{ trx_json_extract('src', ['amnt_rpc1'], type='float', alias='amnt_rpc1') }},
        {{ trx_json_extract('src', ['amnt_rpc2'], type='float', alias='amnt_rpc2') }},
        {{ trx_json_extract('src', ['amnt_refc'], type='float', alias='amnt_refc') }},
        {{ trx_json_extract('src', ['amnt_dwhc'], type='float', alias='amnt_dwhc') }},
        {{ trx_json_extract('src', ['sptm_butm'], type='float', alias='sptm_butm') }},
        {{ trx_json_extract('src', ['avtm_butm'], type='float', alias='avtm_butm') }},
        {{ trx_json_extract('src', ['cstm_butm'], type='float', alias='cstm_butm') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm139') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'term', 'cfln', 'cctp', 'cotp', 'nrbt', 'cseq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
