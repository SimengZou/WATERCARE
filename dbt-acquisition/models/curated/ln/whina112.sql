{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whina112 Inventory Receipt Transactions table, Generated 2026-04-10T19:42:43Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['trdt'], type='timestamp_ntz', alias='trdt') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['inwp'], type='int', alias='inwp') }},
        {{ trx_json_extract('src', ['inwp_kw'], type='varchar', alias='inwp_kw') }},
        {{ trx_json_extract('src', ['atse'], type='varchar', alias='atse') }},
        {{ trx_json_extract('src', ['qstk'], type='float', alias='qstk') }},
        {{ trx_json_extract('src', ['qskt'], type='float', alias='qskt') }},
        {{ trx_json_extract('src', ['serl'], type='varchar', alias='serl') }},
        {{ trx_json_extract('src', ['clot'], type='varchar', alias='clot') }},
        {{ trx_json_extract('src', ['tagn'], type='varchar', alias='tagn') }},
        {{ trx_json_extract('src', ['lgdt'], type='timestamp_ntz', alias='lgdt') }},
        {{ trx_json_extract('src', ['ocmp'], type='int', alias='ocmp') }},
        {{ trx_json_extract('src', ['koor'], type='int', alias='koor') }},
        {{ trx_json_extract('src', ['koor_kw'], type='varchar', alias='koor_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['srnb'], type='int', alias='srnb') }},
        {{ trx_json_extract('src', ['dlin'], type='int', alias='dlin') }},
        {{ trx_json_extract('src', ['itid'], type='varchar', alias='itid') }},
        {{ trx_json_extract('src', ['itse'], type='int', alias='itse') }},
        {{ trx_json_extract('src', ['vaco'], type='int', alias='vaco') }},
        {{ trx_json_extract('src', ['vaco_kw'], type='varchar', alias='vaco_kw') }},
        {{ trx_json_extract('src', ['cons'], type='int', alias='cons') }},
        {{ trx_json_extract('src', ['cons_kw'], type='varchar', alias='cons_kw') }},
        {{ trx_json_extract('src', ['wvgr'], type='varchar', alias='wvgr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['owns'], type='int', alias='owns') }},
        {{ trx_json_extract('src', ['owns_kw'], type='varchar', alias='owns_kw') }},
        {{ trx_json_extract('src', ['ownr'], type='varchar', alias='ownr') }},
        {{ trx_json_extract('src', ['bfbp'], type='varchar', alias='bfbp') }},
        {{ trx_json_extract('src', ['reje'], type='int', alias='reje') }},
        {{ trx_json_extract('src', ['reje_kw'], type='varchar', alias='reje_kw') }},
        {{ trx_json_extract('src', ['phtr'], type='int', alias='phtr') }},
        {{ trx_json_extract('src', ['phtr_kw'], type='varchar', alias='phtr_kw') }},
        {{ trx_json_extract('src', ['qhnd'], type='float', alias='qhnd') }},
        {{ trx_json_extract('src', ['qaiu'], type='float', alias='qaiu') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['atse_ref_compnr'], type='int', alias='atse_ref_compnr') }},
        {{ trx_json_extract('src', ['ocmp_ref_compnr'], type='int', alias='ocmp_ref_compnr') }},
        {{ trx_json_extract('src', ['wvgr_ref_compnr'], type='int', alias='wvgr_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whina112') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'item', 'cwar', 'trdt', 'seqn', 'inwp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
