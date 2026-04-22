{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpptc130 Labor Budget Lines table, Generated 2026-04-10T19:42:21Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['sern'], type='int', alias='sern') }},
        {{ trx_json_extract('src', ['task'], type='varchar', alias='task') }},
        {{ trx_json_extract('src', ['ccco'], type='varchar', alias='ccco') }},
        {{ trx_json_extract('src', ['cstl'], type='varchar', alias='cstl') }},
        {{ trx_json_extract('src', ['quan'], type='float', alias='quan') }},
        {{ trx_json_extract('src', ['norm'], type='float', alias='norm') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['sero'], type='int', alias='sero') }},
        {{ trx_json_extract('src', ['cocu'], type='varchar', alias='cocu') }},
        {{ trx_json_extract('src', ['ratc'], type='float', alias='ratc') }},
        {{ trx_json_extract('src', ['rats'], type='float', alias='rats') }},
        {{ trx_json_extract('src', ['sacu'], type='varchar', alias='sacu') }},
        {{ trx_json_extract('src', ['amoc'], type='float', alias='amoc') }},
        {{ trx_json_extract('src', ['amos'], type='float', alias='amos') }},
        {{ trx_json_extract('src', ['btdt'], type='timestamp_ntz', alias='btdt') }},
        {{ trx_json_extract('src', ['dfrc'], type='int', alias='dfrc') }},
        {{ trx_json_extract('src', ['dfrc_kw'], type='varchar', alias='dfrc_kw') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['rtcc_1'], type='float', alias='rtcc_1') }},
        {{ trx_json_extract('src', ['rtcc_2'], type='float', alias='rtcc_2') }},
        {{ trx_json_extract('src', ['rtcc_3'], type='float', alias='rtcc_3') }},
        {{ trx_json_extract('src', ['rtfc_1'], type='int', alias='rtfc_1') }},
        {{ trx_json_extract('src', ['rtfc_2'], type='int', alias='rtfc_2') }},
        {{ trx_json_extract('src', ['rtfc_3'], type='int', alias='rtfc_3') }},
        {{ trx_json_extract('src', ['rtcs_1'], type='float', alias='rtcs_1') }},
        {{ trx_json_extract('src', ['rtcs_2'], type='float', alias='rtcs_2') }},
        {{ trx_json_extract('src', ['rtcs_3'], type='float', alias='rtcs_3') }},
        {{ trx_json_extract('src', ['rtfs_1'], type='int', alias='rtfs_1') }},
        {{ trx_json_extract('src', ['rtfs_2'], type='int', alias='rtfs_2') }},
        {{ trx_json_extract('src', ['rtfs_3'], type='int', alias='rtfs_3') }},
        {{ trx_json_extract('src', ['unrt'], type='varchar', alias='unrt') }},
        {{ trx_json_extract('src', ['rfac'], type='varchar', alias='rfac') }},
        {{ trx_json_extract('src', ['freq'], type='float', alias='freq') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['acln'], type='int', alias='acln') }},
        {{ trx_json_extract('src', ['tsrl'], type='int', alias='tsrl') }},
        {{ trx_json_extract('src', ['tsrl_kw'], type='varchar', alias='tsrl_kw') }},
        {{ trx_json_extract('src', ['trts'], type='int', alias='trts') }},
        {{ trx_json_extract('src', ['trts_kw'], type='varchar', alias='trts_kw') }},
        {{ trx_json_extract('src', ['cona'], type='float', alias='cona') }},
        {{ trx_json_extract('src', ['eqan'], type='float', alias='eqan') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cprj_cstl_ref_compnr'], type='int', alias='cprj_cstl_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cspa_ref_compnr'], type='int', alias='cprj_cspa_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cspa_sero_ref_compnr'], type='int', alias='cprj_cspa_sero_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_ref_compnr'], type='int', alias='cprj_cpla_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ccco_ref_compnr'], type='int', alias='ccco_ref_compnr') }},
        {{ trx_json_extract('src', ['cuni_ref_compnr'], type='int', alias='cuni_ref_compnr') }},
        {{ trx_json_extract('src', ['cocu_ref_compnr'], type='int', alias='cocu_ref_compnr') }},
        {{ trx_json_extract('src', ['sacu_ref_compnr'], type='int', alias='sacu_ref_compnr') }},
        {{ trx_json_extract('src', ['unrt_ref_compnr'], type='int', alias='unrt_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpptc130') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'cspa', 'sern']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
