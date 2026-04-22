{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam200 Categories table, Generated 2026-04-10T19:41:32Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['ctgy'], type='varchar', alias='ctgy') }},
        {{ trx_json_extract('src', ['agrp'], type='varchar', alias='agrp') }},
        {{ trx_json_extract('src', ['amth'], type='varchar', alias='amth') }},
        {{ trx_json_extract('src', ['aslf'], type='int', alias='aslf') }},
        {{ trx_json_extract('src', ['bslv'], type='int', alias='bslv') }},
        {{ trx_json_extract('src', ['bslv_kw'], type='varchar', alias='bslv_kw') }},
        {{ trx_json_extract('src', ['cmth'], type='varchar', alias='cmth') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['fmth'], type='varchar', alias='fmth') }},
        {{ trx_json_extract('src', ['itcd'], type='int', alias='itcd') }},
        {{ trx_json_extract('src', ['itcd_kw'], type='varchar', alias='itcd_kw') }},
        {{ trx_json_extract('src', ['lmth'], type='varchar', alias='lmth') }},
        {{ trx_json_extract('src', ['life'], type='int', alias='life') }},
        {{ trx_json_extract('src', ['mmth'], type='varchar', alias='mmth') }},
        {{ trx_json_extract('src', ['omth'], type='varchar', alias='omth') }},
        {{ trx_json_extract('src', ['prop'], type='varchar', alias='prop') }},
        {{ trx_json_extract('src', ['sctg'], type='varchar', alias='sctg') }},
        {{ trx_json_extract('src', ['smth'], type='varchar', alias='smth') }},
        {{ trx_json_extract('src', ['tmth'], type='varchar', alias='tmth') }},
        {{ trx_json_extract('src', ['umth'], type='varchar', alias='umth') }},
        {{ trx_json_extract('src', ['dscr'], type='int', alias='dscr') }},
        {{ trx_json_extract('src', ['dscr_kw'], type='varchar', alias='dscr_kw') }},
        {{ trx_json_extract('src', ['fadr'], type='float', alias='fadr') }},
        {{ trx_json_extract('src', ['cthr'], type='float', alias='cthr') }},
        {{ trx_json_extract('src', ['cthc'], type='varchar', alias='cthc') }},
        {{ trx_json_extract('src', ['mskc'], type='int', alias='mskc') }},
        {{ trx_json_extract('src', ['anbm'], type='varchar', alias='anbm') }},
        {{ trx_json_extract('src', ['aexm'], type='varchar', alias='aexm') }},
        {{ trx_json_extract('src', ['agrp_ref_compnr'], type='int', alias='agrp_ref_compnr') }},
        {{ trx_json_extract('src', ['amth_ref_compnr'], type='int', alias='amth_ref_compnr') }},
        {{ trx_json_extract('src', ['cmth_ref_compnr'], type='int', alias='cmth_ref_compnr') }},
        {{ trx_json_extract('src', ['fmth_ref_compnr'], type='int', alias='fmth_ref_compnr') }},
        {{ trx_json_extract('src', ['lmth_ref_compnr'], type='int', alias='lmth_ref_compnr') }},
        {{ trx_json_extract('src', ['mmth_ref_compnr'], type='int', alias='mmth_ref_compnr') }},
        {{ trx_json_extract('src', ['omth_ref_compnr'], type='int', alias='omth_ref_compnr') }},
        {{ trx_json_extract('src', ['prop_ref_compnr'], type='int', alias='prop_ref_compnr') }},
        {{ trx_json_extract('src', ['smth_ref_compnr'], type='int', alias='smth_ref_compnr') }},
        {{ trx_json_extract('src', ['tmth_ref_compnr'], type='int', alias='tmth_ref_compnr') }},
        {{ trx_json_extract('src', ['umth_ref_compnr'], type='int', alias='umth_ref_compnr') }},
        {{ trx_json_extract('src', ['mskc_ref_compnr'], type='int', alias='mskc_ref_compnr') }},
        {{ trx_json_extract('src', ['anbm_ref_compnr'], type='int', alias='anbm_ref_compnr') }},
        {{ trx_json_extract('src', ['aexm_ref_compnr'], type='int', alias='aexm_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam200') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'ctgy']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
