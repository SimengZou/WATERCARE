{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whina116 Inventory Variances table, Generated 2026-04-10T19:42:45Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['trdt'], type='timestamp_ntz', alias='trdt') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['atse'], type='varchar', alias='atse') }},
        {{ trx_json_extract('src', ['stkq'], type='float', alias='stkq') }},
        {{ trx_json_extract('src', ['koor'], type='int', alias='koor') }},
        {{ trx_json_extract('src', ['koor_kw'], type='varchar', alias='koor_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['srnb'], type='int', alias='srnb') }},
        {{ trx_json_extract('src', ['dlin'], type='int', alias='dlin') }},
        {{ trx_json_extract('src', ['ivsq'], type='int', alias='ivsq') }},
        {{ trx_json_extract('src', ['depc'], type='int', alias='depc') }},
        {{ trx_json_extract('src', ['wdep'], type='varchar', alias='wdep') }},
        {{ trx_json_extract('src', ['cbyi'], type='int', alias='cbyi') }},
        {{ trx_json_extract('src', ['cbyi_kw'], type='varchar', alias='cbyi_kw') }},
        {{ trx_json_extract('src', ['proc'], type='int', alias='proc') }},
        {{ trx_json_extract('src', ['proc_kw'], type='varchar', alias='proc_kw') }},
        {{ trx_json_extract('src', ['iror'], type='int', alias='iror') }},
        {{ trx_json_extract('src', ['iror_kw'], type='varchar', alias='iror_kw') }},
        {{ trx_json_extract('src', ['cuso'], type='int', alias='cuso') }},
        {{ trx_json_extract('src', ['cuso_kw'], type='varchar', alias='cuso_kw') }},
        {{ trx_json_extract('src', ['owns'], type='int', alias='owns') }},
        {{ trx_json_extract('src', ['owns_kw'], type='varchar', alias='owns_kw') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['prdt'], type='timestamp_ntz', alias='prdt') }},
        {{ trx_json_extract('src', ['rcno'], type='varchar', alias='rcno') }},
        {{ trx_json_extract('src', ['rcln'], type='int', alias='rcln') }},
        {{ trx_json_extract('src', ['pyps'], type='int', alias='pyps') }},
        {{ trx_json_extract('src', ['lgdt'], type='timestamp_ntz', alias='lgdt') }},
        {{ trx_json_extract('src', ['wvgr'], type='varchar', alias='wvgr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['atse_ref_compnr'], type='int', alias='atse_ref_compnr') }},
        {{ trx_json_extract('src', ['depc_ref_compnr'], type='int', alias='depc_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['wvgr_ref_compnr'], type='int', alias='wvgr_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whina116') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'item', 'cwar', 'trdt', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
