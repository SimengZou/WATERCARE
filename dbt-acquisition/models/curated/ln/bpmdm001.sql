{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN bpmdm001 Employee People Data table, Generated 2026-04-10T19:40:58Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['cadr'], type='varchar', alias='cadr') }},
        {{ trx_json_extract('src', ['hwem'], type='float', alias='hwem') }},
        {{ trx_json_extract('src', ['wtsc'], type='varchar', alias='wtsc') }},
        {{ trx_json_extract('src', ['sdte'], type='date', alias='sdte') }},
        {{ trx_json_extract('src', ['edte'], type='date', alias='edte') }},
        {{ trx_json_extract('src', ['daob'], type='date', alias='daob') }},
        {{ trx_json_extract('src', ['sexe'], type='int', alias='sexe') }},
        {{ trx_json_extract('src', ['sexe_kw'], type='varchar', alias='sexe_kw') }},
        {{ trx_json_extract('src', ['cist'], type='varchar', alias='cist') }},
        {{ trx_json_extract('src', ['emtp'], type='int', alias='emtp') }},
        {{ trx_json_extract('src', ['emtp_kw'], type='varchar', alias='emtp_kw') }},
        {{ trx_json_extract('src', ['finr'], type='varchar', alias='finr') }},
        {{ trx_json_extract('src', ['telw'], type='varchar', alias='telw') }},
        {{ trx_json_extract('src', ['tlw1'], type='varchar', alias='tlw1') }},
        {{ trx_json_extract('src', ['tefw'], type='varchar', alias='tefw') }},
        {{ trx_json_extract('src', ['mail', 'en_US'], type='varchar', alias='mail__en_us') }},
        {{ trx_json_extract('src', ['ucrm'], type='int', alias='ucrm') }},
        {{ trx_json_extract('src', ['ucrm_kw'], type='varchar', alias='ucrm_kw') }},
        {{ trx_json_extract('src', ['telm'], type='varchar', alias='telm') }},
        {{ trx_json_extract('src', ['msty'], type='int', alias='msty') }},
        {{ trx_json_extract('src', ['msty_kw'], type='varchar', alias='msty_kw') }},
        {{ trx_json_extract('src', ['msad', 'en_US'], type='varchar', alias='msad__en_us') }},
        {{ trx_json_extract('src', ['jobt', 'en_US'], type='varchar', alias='jobt__en_us') }},
        {{ trx_json_extract('src', ['bano'], type='varchar', alias='bano') }},
        {{ trx_json_extract('src', ['tctr', 'en_US'], type='varchar', alias='tctr__en_us') }},
        {{ trx_json_extract('src', ['cedt'], type='date', alias='cedt') }},
        {{ trx_json_extract('src', ['otbp'], type='varchar', alias='otbp') }},
        {{ trx_json_extract('src', ['ccty'], type='varchar', alias='ccty') }},
        {{ trx_json_extract('src', ['rgno', 'en_US'], type='varchar', alias='rgno__en_us') }},
        {{ trx_json_extract('src', ['prtn', 'en_US'], type='varchar', alias='prtn__en_us') }},
        {{ trx_json_extract('src', ['ptbp'], type='varchar', alias='ptbp') }},
        {{ trx_json_extract('src', ['supv'], type='varchar', alias='supv') }},
        {{ trx_json_extract('src', ['ctrg'], type='varchar', alias='ctrg') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['cadr_ref_compnr'], type='int', alias='cadr_ref_compnr') }},
        {{ trx_json_extract('src', ['wtsc_ref_compnr'], type='int', alias='wtsc_ref_compnr') }},
        {{ trx_json_extract('src', ['otbp_ref_compnr'], type='int', alias='otbp_ref_compnr') }},
        {{ trx_json_extract('src', ['ccty_ref_compnr'], type='int', alias='ccty_ref_compnr') }},
        {{ trx_json_extract('src', ['ptbp_ref_compnr'], type='int', alias='ptbp_ref_compnr') }},
        {{ trx_json_extract('src', ['supv_ref_compnr'], type='int', alias='supv_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_bpmdm001') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'emno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
