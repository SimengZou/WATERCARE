{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whina122 Inventory Revaluation Transactions table, Generated 2026-04-10T19:42:46Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['rorg'], type='int', alias='rorg') }},
        {{ trx_json_extract('src', ['rorg_kw'], type='varchar', alias='rorg_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['atse'], type='varchar', alias='atse') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['rowc'], type='int', alias='rowc') }},
        {{ trx_json_extract('src', ['rown'], type='varchar', alias='rown') }},
        {{ trx_json_extract('src', ['rvrs'], type='varchar', alias='rvrs') }},
        {{ trx_json_extract('src', ['rvdt'], type='timestamp_ntz', alias='rvdt') }},
        {{ trx_json_extract('src', ['lgdt'], type='timestamp_ntz', alias='lgdt') }},
        {{ trx_json_extract('src', ['logn'], type='varchar', alias='logn') }},
        {{ trx_json_extract('src', ['quan'], type='float', alias='quan') }},
        {{ trx_json_extract('src', ['serl'], type='varchar', alias='serl') }},
        {{ trx_json_extract('src', ['clot'], type='varchar', alias='clot') }},
        {{ trx_json_extract('src', ['tagn'], type='varchar', alias='tagn') }},
        {{ trx_json_extract('src', ['inwp'], type='int', alias='inwp') }},
        {{ trx_json_extract('src', ['inwp_kw'], type='varchar', alias='inwp_kw') }},
        {{ trx_json_extract('src', ['ovba'], type='int', alias='ovba') }},
        {{ trx_json_extract('src', ['ovba_kw'], type='varchar', alias='ovba_kw') }},
        {{ trx_json_extract('src', ['oval'], type='int', alias='oval') }},
        {{ trx_json_extract('src', ['oval_kw'], type='varchar', alias='oval_kw') }},
        {{ trx_json_extract('src', ['owvg'], type='int', alias='owvg') }},
        {{ trx_json_extract('src', ['owvg_kw'], type='varchar', alias='owvg_kw') }},
        {{ trx_json_extract('src', ['ovgr'], type='varchar', alias='ovgr') }},
        {{ trx_json_extract('src', ['nvba'], type='int', alias='nvba') }},
        {{ trx_json_extract('src', ['nvba_kw'], type='varchar', alias='nvba_kw') }},
        {{ trx_json_extract('src', ['nval'], type='int', alias='nval') }},
        {{ trx_json_extract('src', ['nval_kw'], type='varchar', alias='nval_kw') }},
        {{ trx_json_extract('src', ['nwvg'], type='int', alias='nwvg') }},
        {{ trx_json_extract('src', ['nwvg_kw'], type='varchar', alias='nwvg_kw') }},
        {{ trx_json_extract('src', ['nvgr'], type='varchar', alias='nvgr') }},
        {{ trx_json_extract('src', ['cvmo'], type='int', alias='cvmo') }},
        {{ trx_json_extract('src', ['cvmo_kw'], type='varchar', alias='cvmo_kw') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['cstl'], type='varchar', alias='cstl') }},
        {{ trx_json_extract('src', ['ccco'], type='varchar', alias='ccco') }},
        {{ trx_json_extract('src', ['owns'], type='int', alias='owns') }},
        {{ trx_json_extract('src', ['owns_kw'], type='varchar', alias='owns_kw') }},
        {{ trx_json_extract('src', ['ownr'], type='varchar', alias='ownr') }},
        {{ trx_json_extract('src', ['bfbp'], type='varchar', alias='bfbp') }},
        {{ trx_json_extract('src', ['reje'], type='int', alias='reje') }},
        {{ trx_json_extract('src', ['reje_kw'], type='varchar', alias='reje_kw') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['atse_ref_compnr'], type='int', alias='atse_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['rowc_ref_compnr'], type='int', alias='rowc_ref_compnr') }},
        {{ trx_json_extract('src', ['rown_ref_compnr'], type='int', alias='rown_ref_compnr') }},
        {{ trx_json_extract('src', ['rvrs_ref_compnr'], type='int', alias='rvrs_ref_compnr') }},
        {{ trx_json_extract('src', ['ovgr_ref_compnr'], type='int', alias='ovgr_ref_compnr') }},
        {{ trx_json_extract('src', ['nvgr_ref_compnr'], type='int', alias='nvgr_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ccco_ref_compnr'], type='int', alias='ccco_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whina122') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'rorg', 'orno', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
