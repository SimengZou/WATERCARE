{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffbs005 Budgets per Year table, Generated 2026-04-10T19:41:37Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['budg'], type='varchar', alias='budg') }},
        {{ trx_json_extract('src', ['pbyr'], type='int', alias='pbyr') }},
        {{ trx_json_extract('src', ['pbud'], type='varchar', alias='pbud') }},
        {{ trx_json_extract('src', ['dbyr'], type='int', alias='dbyr') }},
        {{ trx_json_extract('src', ['dbud'], type='varchar', alias='dbud') }},
        {{ trx_json_extract('src', ['defi'], type='int', alias='defi') }},
        {{ trx_json_extract('src', ['defi_kw'], type='varchar', alias='defi_kw') }},
        {{ trx_json_extract('src', ['disb'], type='varchar', alias='disb') }},
        {{ trx_json_extract('src', ['lmdt'], type='timestamp_ntz', alias='lmdt') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['ratc_1'], type='float', alias='ratc_1') }},
        {{ trx_json_extract('src', ['ratc_2'], type='float', alias='ratc_2') }},
        {{ trx_json_extract('src', ['ratc_3'], type='float', alias='ratc_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['cbyr'], type='int', alias='cbyr') }},
        {{ trx_json_extract('src', ['cbud'], type='varchar', alias='cbud') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['budg_ref_compnr'], type='int', alias='budg_ref_compnr') }},
        {{ trx_json_extract('src', ['pbyr_pbud_ref_compnr'], type='int', alias='pbyr_pbud_ref_compnr') }},
        {{ trx_json_extract('src', ['pbud_ref_compnr'], type='int', alias='pbud_ref_compnr') }},
        {{ trx_json_extract('src', ['dbyr_dbud_ref_compnr'], type='int', alias='dbyr_dbud_ref_compnr') }},
        {{ trx_json_extract('src', ['dbud_ref_compnr'], type='int', alias='dbud_ref_compnr') }},
        {{ trx_json_extract('src', ['disb_ref_compnr'], type='int', alias='disb_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['cbyr_cbud_ref_compnr'], type='int', alias='cbyr_cbud_ref_compnr') }},
        {{ trx_json_extract('src', ['cbud_ref_compnr'], type='int', alias='cbud_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffbs005') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'year', 'budg']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
