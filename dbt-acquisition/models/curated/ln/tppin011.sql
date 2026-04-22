{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppin011 Settled Advance Payments table, Generated 2026-04-10T19:42:04Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cono'], type='varchar', alias='cono') }},
        {{ trx_json_extract('src', ['cnln'], type='varchar', alias='cnln') }},
        {{ trx_json_extract('src', ['sern'], type='int', alias='sern') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['ofbp'], type='varchar', alias='ofbp') }},
        {{ trx_json_extract('src', ['serl'], type='int', alias='serl') }},
        {{ trx_json_extract('src', ['cnpr'], type='varchar', alias='cnpr') }},
        {{ trx_json_extract('src', ['amnt'], type='float', alias='amnt') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['rate_1'], type='float', alias='rate_1') }},
        {{ trx_json_extract('src', ['rate_2'], type='float', alias='rate_2') }},
        {{ trx_json_extract('src', ['rate_3'], type='float', alias='rate_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['rdat'], type='timestamp_ntz', alias='rdat') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['pamt'], type='float', alias='pamt') }},
        {{ trx_json_extract('src', ['fcmp'], type='int', alias='fcmp') }},
        {{ trx_json_extract('src', ['ityp'], type='varchar', alias='ityp') }},
        {{ trx_json_extract('src', ['idoc'], type='int', alias='idoc') }},
        {{ trx_json_extract('src', ['idln'], type='int', alias='idln') }},
        {{ trx_json_extract('src', ['iseq'], type='int', alias='iseq') }},
        {{ trx_json_extract('src', ['adva'], type='int', alias='adva') }},
        {{ trx_json_extract('src', ['nins'], type='int', alias='nins') }},
        {{ trx_json_extract('src', ['dlvr'], type='int', alias='dlvr') }},
        {{ trx_json_extract('src', ['schd'], type='int', alias='schd') }},
        {{ trx_json_extract('src', ['prdt'], type='timestamp_ntz', alias='prdt') }},
        {{ trx_json_extract('src', ['cono_cnln_sern_cprj_ofbp_ref_compnr'], type='int', alias='cono_cnln_sern_cprj_ofbp_ref_compnr') }},
        {{ trx_json_extract('src', ['cono_cnln_ref_compnr'], type='int', alias='cono_cnln_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ofbp_ref_compnr'], type='int', alias='ofbp_ref_compnr') }},
        {{ trx_json_extract('src', ['cnpr_ref_compnr'], type='int', alias='cnpr_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['rtyp_ref_compnr'], type='int', alias='rtyp_ref_compnr') }},
        {{ trx_json_extract('src', ['amnt_refc'], type='float', alias='amnt_refc') }},
        {{ trx_json_extract('src', ['amnt_cntc'], type='float', alias='amnt_cntc') }},
        {{ trx_json_extract('src', ['amnt_prjc'], type='float', alias='amnt_prjc') }},
        {{ trx_json_extract('src', ['amnt_homc'], type='float', alias='amnt_homc') }},
        {{ trx_json_extract('src', ['amnt_rpc1'], type='float', alias='amnt_rpc1') }},
        {{ trx_json_extract('src', ['amnt_rpc2'], type='float', alias='amnt_rpc2') }},
        {{ trx_json_extract('src', ['amnt_dwhc'], type='float', alias='amnt_dwhc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppin011') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cono', 'cnln', 'sern', 'cprj', 'ofbp', 'serl']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
