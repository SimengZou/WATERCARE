{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm400 Contract Installments table, Generated 2026-04-10T19:42:34Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['csco'], type='varchar', alias='csco') }},
        {{ trx_json_extract('src', ['inst'], type='int', alias='inst') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['stin'], type='int', alias='stin') }},
        {{ trx_json_extract('src', ['stin_kw'], type='varchar', alias='stin_kw') }},
        {{ trx_json_extract('src', ['pidt'], type='timestamp_ntz', alias='pidt') }},
        {{ trx_json_extract('src', ['efdt'], type='timestamp_ntz', alias='efdt') }},
        {{ trx_json_extract('src', ['exdt'], type='timestamp_ntz', alias='exdt') }},
        {{ trx_json_extract('src', ['isst'], type='int', alias='isst') }},
        {{ trx_json_extract('src', ['isst_kw'], type='varchar', alias='isst_kw') }},
        {{ trx_json_extract('src', ['amnt'], type='float', alias='amnt') }},
        {{ trx_json_extract('src', ['disc'], type='float', alias='disc') }},
        {{ trx_json_extract('src', ['dimt'], type='float', alias='dimt') }},
        {{ trx_json_extract('src', ['ntmt'], type='float', alias='ntmt') }},
        {{ trx_json_extract('src', ['icmt'], type='float', alias='icmt') }},
        {{ trx_json_extract('src', ['inmt'], type='float', alias='inmt') }},
        {{ trx_json_extract('src', ['txmt'], type='float', alias='txmt') }},
        {{ trx_json_extract('src', ['fcpc'], type='float', alias='fcpc') }},
        {{ trx_json_extract('src', ['fcmt_1'], type='float', alias='fcmt_1') }},
        {{ trx_json_extract('src', ['fcmt_2'], type='float', alias='fcmt_2') }},
        {{ trx_json_extract('src', ['fcmt_3'], type='float', alias='fcmt_3') }},
        {{ trx_json_extract('src', ['rahc_1'], type='float', alias='rahc_1') }},
        {{ trx_json_extract('src', ['rahc_2'], type='float', alias='rahc_2') }},
        {{ trx_json_extract('src', ['rahc_3'], type='float', alias='rahc_3') }},
        {{ trx_json_extract('src', ['ratc_1'], type='float', alias='ratc_1') }},
        {{ trx_json_extract('src', ['ratc_2'], type='float', alias='ratc_2') }},
        {{ trx_json_extract('src', ['ratc_3'], type='float', alias='ratc_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['ratd'], type='timestamp_ntz', alias='ratd') }},
        {{ trx_json_extract('src', ['nrpf'], type='int', alias='nrpf') }},
        {{ trx_json_extract('src', ['indt'], type='timestamp_ntz', alias='indt') }},
        {{ trx_json_extract('src', ['icmp'], type='int', alias='icmp') }},
        {{ trx_json_extract('src', ['ityp'], type='varchar', alias='ityp') }},
        {{ trx_json_extract('src', ['idoc'], type='int', alias='idoc') }},
        {{ trx_json_extract('src', ['fyer'], type='int', alias='fyer') }},
        {{ trx_json_extract('src', ['fper'], type='int', alias='fper') }},
        {{ trx_json_extract('src', ['prdt'], type='timestamp_ntz', alias='prdt') }},
        {{ trx_json_extract('src', ['istd'], type='date', alias='istd') }},
        {{ trx_json_extract('src', ['cchn'], type='int', alias='cchn') }},
        {{ trx_json_extract('src', ['term'], type='int', alias='term') }},
        {{ trx_json_extract('src', ['cfln'], type='int', alias='cfln') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['csco_cchn_ref_compnr'], type='int', alias='csco_cchn_ref_compnr') }},
        {{ trx_json_extract('src', ['csco_ref_compnr'], type='int', alias='csco_ref_compnr') }},
        {{ trx_json_extract('src', ['term_cfln_ref_compnr'], type='int', alias='term_cfln_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['amnt_homc'], type='float', alias='amnt_homc') }},
        {{ trx_json_extract('src', ['amnt_rpc1'], type='float', alias='amnt_rpc1') }},
        {{ trx_json_extract('src', ['amnt_rpc2'], type='float', alias='amnt_rpc2') }},
        {{ trx_json_extract('src', ['amnt_refc'], type='float', alias='amnt_refc') }},
        {{ trx_json_extract('src', ['amnt_dwhc'], type='float', alias='amnt_dwhc') }},
        {{ trx_json_extract('src', ['dimt_homc'], type='float', alias='dimt_homc') }},
        {{ trx_json_extract('src', ['dimt_rpc1'], type='float', alias='dimt_rpc1') }},
        {{ trx_json_extract('src', ['dimt_rpc2'], type='float', alias='dimt_rpc2') }},
        {{ trx_json_extract('src', ['dimt_refc'], type='float', alias='dimt_refc') }},
        {{ trx_json_extract('src', ['dimt_dwhc'], type='float', alias='dimt_dwhc') }},
        {{ trx_json_extract('src', ['rahc_cntc'], type='float', alias='rahc_cntc') }},
        {{ trx_json_extract('src', ['rahc_refc'], type='float', alias='rahc_refc') }},
        {{ trx_json_extract('src', ['rahc_dwhc'], type='float', alias='rahc_dwhc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm400') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'csco', 'inst']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
