{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpptc147 Control Data Equipment Lines table, Generated 2026-04-10T19:42:22Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cequ'], type='varchar', alias='cequ') }},
        {{ trx_json_extract('src', ['sern'], type='int', alias='sern') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['cceq'], type='varchar', alias='cceq') }},
        {{ trx_json_extract('src', ['ccco'], type='varchar', alias='ccco') }},
        {{ trx_json_extract('src', ['cstl'], type='varchar', alias='cstl') }},
        {{ trx_json_extract('src', ['bsqn'], type='int', alias='bsqn') }},
        {{ trx_json_extract('src', ['eqan'], type='float', alias='eqan') }},
        {{ trx_json_extract('src', ['qutm'], type='float', alias='qutm') }},
        {{ trx_json_extract('src', ['pric'], type='float', alias='pric') }},
        {{ trx_json_extract('src', ['amoc'], type='float', alias='amoc') }},
        {{ trx_json_extract('src', ['cocu'], type='varchar', alias='cocu') }},
        {{ trx_json_extract('src', ['dfrt'], type='float', alias='dfrt') }},
        {{ trx_json_extract('src', ['rtcc_1'], type='float', alias='rtcc_1') }},
        {{ trx_json_extract('src', ['rtcc_2'], type='float', alias='rtcc_2') }},
        {{ trx_json_extract('src', ['rtcc_3'], type='float', alias='rtcc_3') }},
        {{ trx_json_extract('src', ['rtfc_1'], type='int', alias='rtfc_1') }},
        {{ trx_json_extract('src', ['rtfc_2'], type='int', alias='rtfc_2') }},
        {{ trx_json_extract('src', ['rtfc_3'], type='int', alias='rtfc_3') }},
        {{ trx_json_extract('src', ['exeq'], type='int', alias='exeq') }},
        {{ trx_json_extract('src', ['exeq_kw'], type='varchar', alias='exeq_kw') }},
        {{ trx_json_extract('src', ['prdt'], type='timestamp_ntz', alias='prdt') }},
        {{ trx_json_extract('src', ['shft'], type='int', alias='shft') }},
        {{ trx_json_extract('src', ['fidt'], type='timestamp_ntz', alias='fidt') }},
        {{ trx_json_extract('src', ['exep'], type='int', alias='exep') }},
        {{ trx_json_extract('src', ['exep_kw'], type='varchar', alias='exep_kw') }},
        {{ trx_json_extract('src', ['btdt'], type='timestamp_ntz', alias='btdt') }},
        {{ trx_json_extract('src', ['tsrl'], type='int', alias='tsrl') }},
        {{ trx_json_extract('src', ['tsrl_kw'], type='varchar', alias='tsrl_kw') }},
        {{ trx_json_extract('src', ['prby'], type='int', alias='prby') }},
        {{ trx_json_extract('src', ['prby_kw'], type='varchar', alias='prby_kw') }},
        {{ trx_json_extract('src', ['eqty'], type='int', alias='eqty') }},
        {{ trx_json_extract('src', ['eqty_kw'], type='varchar', alias='eqty_kw') }},
        {{ trx_json_extract('src', ['otbp'], type='varchar', alias='otbp') }},
        {{ trx_json_extract('src', ['cprj_cstl_ref_compnr'], type='int', alias='cprj_cstl_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cspa_ref_compnr'], type='int', alias='cprj_cspa_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ccco_ref_compnr'], type='int', alias='ccco_ref_compnr') }},
        {{ trx_json_extract('src', ['cocu_ref_compnr'], type='int', alias='cocu_ref_compnr') }},
        {{ trx_json_extract('src', ['otbp_ref_compnr'], type='int', alias='otbp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpptc147') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'cequ', 'sern']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
