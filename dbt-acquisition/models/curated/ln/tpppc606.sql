{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpppc606 Overhead Cost Forecast table, Generated 2026-04-10T19:42:17Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['rgdt'], type='timestamp_ntz', alias='rgdt') }},
        {{ trx_json_extract('src', ['ovhd'], type='varchar', alias='ovhd') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['amoc'], type='float', alias='amoc') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['ccco'], type='varchar', alias='ccco') }},
        {{ trx_json_extract('src', ['rtcc_1'], type='float', alias='rtcc_1') }},
        {{ trx_json_extract('src', ['rtcc_2'], type='float', alias='rtcc_2') }},
        {{ trx_json_extract('src', ['rtcc_3'], type='float', alias='rtcc_3') }},
        {{ trx_json_extract('src', ['rtfc_1'], type='int', alias='rtfc_1') }},
        {{ trx_json_extract('src', ['rtfc_2'], type='int', alias='rtfc_2') }},
        {{ trx_json_extract('src', ['rtfc_3'], type='int', alias='rtfc_3') }},
        {{ trx_json_extract('src', ['rdat'], type='timestamp_ntz', alias='rdat') }},
        {{ trx_json_extract('src', ['appr'], type='int', alias='appr') }},
        {{ trx_json_extract('src', ['appr_kw'], type='varchar', alias='appr_kw') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cprj_cspa_ref_compnr'], type='int', alias='cprj_cspa_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ovhd_ref_compnr'], type='int', alias='ovhd_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['ccco_ref_compnr'], type='int', alias='ccco_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['amoc_cntc'], type='float', alias='amoc_cntc') }},
        {{ trx_json_extract('src', ['amoc_dwhc'], type='float', alias='amoc_dwhc') }},
        {{ trx_json_extract('src', ['amoc_refc'], type='float', alias='amoc_refc') }},
        {{ trx_json_extract('src', ['amoc_homc'], type='float', alias='amoc_homc') }},
        {{ trx_json_extract('src', ['amoc_rpc1'], type='float', alias='amoc_rpc1') }},
        {{ trx_json_extract('src', ['amoc_rpc2'], type='float', alias='amoc_rpc2') }},
        {{ trx_json_extract('src', ['amoc_prjc'], type='float', alias='amoc_prjc') }},
        {{ trx_json_extract('src', ['etoc_cntc'], type='float', alias='etoc_cntc') }},
        {{ trx_json_extract('src', ['etoc_dwhc'], type='float', alias='etoc_dwhc') }},
        {{ trx_json_extract('src', ['etoc_refc'], type='float', alias='etoc_refc') }},
        {{ trx_json_extract('src', ['etoc_homc'], type='float', alias='etoc_homc') }},
        {{ trx_json_extract('src', ['etoc_rpc1'], type='float', alias='etoc_rpc1') }},
        {{ trx_json_extract('src', ['etoc_rpc2'], type='float', alias='etoc_rpc2') }},
        {{ trx_json_extract('src', ['etoc_prjc'], type='float', alias='etoc_prjc') }},
        {{ trx_json_extract('src', ['etoc_trnc'], type='float', alias='etoc_trnc') }},
        {{ trx_json_extract('src', ['eatc_cntc'], type='float', alias='eatc_cntc') }},
        {{ trx_json_extract('src', ['eatc_dwhc'], type='float', alias='eatc_dwhc') }},
        {{ trx_json_extract('src', ['eatc_refc'], type='float', alias='eatc_refc') }},
        {{ trx_json_extract('src', ['eatc_homc'], type='float', alias='eatc_homc') }},
        {{ trx_json_extract('src', ['eatc_rpc1'], type='float', alias='eatc_rpc1') }},
        {{ trx_json_extract('src', ['eatc_rpc2'], type='float', alias='eatc_rpc2') }},
        {{ trx_json_extract('src', ['eatc_prjc'], type='float', alias='eatc_prjc') }},
        {{ trx_json_extract('src', ['eatc_trnc'], type='float', alias='eatc_trnc') }},
        {{ trx_json_extract('src', ['cprj_fcmp'], type='int', alias='cprj_fcmp') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpppc606') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'rgdt', 'ovhd', 'cspa', 'cact']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
