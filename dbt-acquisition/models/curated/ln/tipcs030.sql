{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tipcs030 Project Details table, Generated 2026-04-10T19:41:48Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['psts'], type='int', alias='psts') }},
        {{ trx_json_extract('src', ['psts_kw'], type='varchar', alias='psts_kw') }},
        {{ trx_json_extract('src', ['psta'], type='varchar', alias='psta') }},
        {{ trx_json_extract('src', ['sdat'], type='timestamp_ntz', alias='sdat') }},
        {{ trx_json_extract('src', ['ddat'], type='timestamp_ntz', alias='ddat') }},
        {{ trx_json_extract('src', ['intp'], type='int', alias='intp') }},
        {{ trx_json_extract('src', ['intp_kw'], type='varchar', alias='intp_kw') }},
        {{ trx_json_extract('src', ['erev'], type='int', alias='erev') }},
        {{ trx_json_extract('src', ['erev_kw'], type='varchar', alias='erev_kw') }},
        {{ trx_json_extract('src', ['bkcs'], type='int', alias='bkcs') }},
        {{ trx_json_extract('src', ['bkcs_kw'], type='varchar', alias='bkcs_kw') }},
        {{ trx_json_extract('src', ['bkdt'], type='timestamp_ntz', alias='bkdt') }},
        {{ trx_json_extract('src', ['tinv_1'], type='float', alias='tinv_1') }},
        {{ trx_json_extract('src', ['tinv_2'], type='float', alias='tinv_2') }},
        {{ trx_json_extract('src', ['tinv_3'], type='float', alias='tinv_3') }},
        {{ trx_json_extract('src', ['ascp'], type='float', alias='ascp') }},
        {{ trx_json_extract('src', ['ecpa'], type='float', alias='ecpa') }},
        {{ trx_json_extract('src', ['ecpr'], type='float', alias='ecpr') }},
        {{ trx_json_extract('src', ['cbdg'], type='varchar', alias='cbdg') }},
        {{ trx_json_extract('src', ['plcd'], type='int', alias='plcd') }},
        {{ trx_json_extract('src', ['plcd_kw'], type='varchar', alias='plcd_kw') }},
        {{ trx_json_extract('src', ['defc'], type='int', alias='defc') }},
        {{ trx_json_extract('src', ['defc_kw'], type='varchar', alias='defc_kw') }},
        {{ trx_json_extract('src', ['cldt'], type='timestamp_ntz', alias='cldt') }},
        {{ trx_json_extract('src', ['mib4'], type='int', alias='mib4') }},
        {{ trx_json_extract('src', ['mib4_kw'], type='varchar', alias='mib4_kw') }},
        {{ trx_json_extract('src', ['soco'], type='float', alias='soco') }},
        {{ trx_json_extract('src', ['pocm'], type='int', alias='pocm') }},
        {{ trx_json_extract('src', ['pocm_kw'], type='varchar', alias='pocm_kw') }},
        {{ trx_json_extract('src', ['intc'], type='int', alias='intc') }},
        {{ trx_json_extract('src', ['intc_kw'], type='varchar', alias='intc_kw') }},
        {{ trx_json_extract('src', ['incp'], type='varchar', alias='incp') }},
        {{ trx_json_extract('src', ['rrtp'], type='int', alias='rrtp') }},
        {{ trx_json_extract('src', ['rrtp_kw'], type='varchar', alias='rrtp_kw') }},
        {{ trx_json_extract('src', ['otac'], type='int', alias='otac') }},
        {{ trx_json_extract('src', ['otac_kw'], type='varchar', alias='otac_kw') }},
        {{ trx_json_extract('src', ['acdt'], type='timestamp_ntz', alias='acdt') }},
        {{ trx_json_extract('src', ['crfc'], type='int', alias='crfc') }},
        {{ trx_json_extract('src', ['crfc_kw'], type='varchar', alias='crfc_kw') }},
        {{ trx_json_extract('src', ['dstr'], type='int', alias='dstr') }},
        {{ trx_json_extract('src', ['dstr_kw'], type='varchar', alias='dstr_kw') }},
        {{ trx_json_extract('src', ['tpsr'], type='int', alias='tpsr') }},
        {{ trx_json_extract('src', ['tpsr_kw'], type='varchar', alias='tpsr_kw') }},
        {{ trx_json_extract('src', ['aman'], type='float', alias='aman') }},
        {{ trx_json_extract('src', ['amch'], type='float', alias='amch') }},
        {{ trx_json_extract('src', ['ncac'], type='int', alias='ncac') }},
        {{ trx_json_extract('src', ['ncac_kw'], type='varchar', alias='ncac_kw') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['psta_ref_compnr'], type='int', alias='psta_ref_compnr') }},
        {{ trx_json_extract('src', ['cbdg_ref_compnr'], type='int', alias='cbdg_ref_compnr') }},
        {{ trx_json_extract('src', ['incp_ref_compnr'], type='int', alias='incp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tipcs030') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
