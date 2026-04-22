{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppin050 Transferred Unit Rate Invoice Lines table, Generated 2026-04-10T19:42:05Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['sern'], type='int', alias='sern') }},
        {{ trx_json_extract('src', ['pgdt'], type='timestamp_ntz', alias='pgdt') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['quan'], type='float', alias='quan') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['amos'], type='float', alias='amos') }},
        {{ trx_json_extract('src', ['rate_1'], type='float', alias='rate_1') }},
        {{ trx_json_extract('src', ['rate_2'], type='float', alias='rate_2') }},
        {{ trx_json_extract('src', ['rate_3'], type='float', alias='rate_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['rdat'], type='timestamp_ntz', alias='rdat') }},
        {{ trx_json_extract('src', ['fcmp'], type='int', alias='fcmp') }},
        {{ trx_json_extract('src', ['ityp'], type='varchar', alias='ityp') }},
        {{ trx_json_extract('src', ['idoc'], type='int', alias='idoc') }},
        {{ trx_json_extract('src', ['cprj_cspa_ref_compnr'], type='int', alias='cprj_cspa_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['cuni_ref_compnr'], type='int', alias='cuni_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['amos_refc'], type='float', alias='amos_refc') }},
        {{ trx_json_extract('src', ['amos_cntc'], type='float', alias='amos_cntc') }},
        {{ trx_json_extract('src', ['amos_prjc'], type='float', alias='amos_prjc') }},
        {{ trx_json_extract('src', ['amos_homc'], type='float', alias='amos_homc') }},
        {{ trx_json_extract('src', ['amos_rpc1'], type='float', alias='amos_rpc1') }},
        {{ trx_json_extract('src', ['amos_rpc2'], type='float', alias='amos_rpc2') }},
        {{ trx_json_extract('src', ['amos_dwhc'], type='float', alias='amos_dwhc') }},
        {{ trx_json_extract('src', ['quan_buar'], type='float', alias='quan_buar') }},
        {{ trx_json_extract('src', ['quan_buvl'], type='float', alias='quan_buvl') }},
        {{ trx_json_extract('src', ['quan_buln'], type='float', alias='quan_buln') }},
        {{ trx_json_extract('src', ['quan_bupc'], type='float', alias='quan_bupc') }},
        {{ trx_json_extract('src', ['quan_buwg'], type='float', alias='quan_buwg') }},
        {{ trx_json_extract('src', ['quan_butm'], type='float', alias='quan_butm') }},
        {{ trx_json_extract('src', ['quan_hour'], type='float', alias='quan_hour') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppin050') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'cspa', 'cpla', 'cact', 'sern']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
