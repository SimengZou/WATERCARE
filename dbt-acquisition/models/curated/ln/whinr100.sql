{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinr100 Inventory Transactions by Stock Point table, Generated 2025-06-12T01:48:43Z from package combination ce01090',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['clot'], type='varchar', alias='clot') }},
        {{ trx_json_extract('src', ['serl'], type='varchar', alias='serl') }},
        {{ trx_json_extract('src', ['idat'], type='timestamp_ntz', alias='idat') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['loca'], type='varchar', alias='loca') }},
        {{ trx_json_extract('src', ['trdt'], type='timestamp_ntz', alias='trdt') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['atse'], type='varchar', alias='atse') }},
        {{ trx_json_extract('src', ['tagn'], type='varchar', alias='tagn') }},
        {{ trx_json_extract('src', ['koor'], type='int', alias='koor') }},
        {{ trx_json_extract('src', ['koor_kw'], type='varchar', alias='koor_kw') }},
        {{ trx_json_extract('src', ['kost'], type='int', alias='kost') }},
        {{ trx_json_extract('src', ['kost_kw'], type='varchar', alias='kost_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['prnt'], type='int', alias='prnt') }},
        {{ trx_json_extract('src', ['prnt_kw'], type='varchar', alias='prnt_kw') }},
        {{ trx_json_extract('src', ['qstk'], type='float', alias='qstk') }},
        {{ trx_json_extract('src', ['qipu'], type='float', alias='qipu') }},
        {{ trx_json_extract('src', ['qhnd'], type='float', alias='qhnd') }},
        {{ trx_json_extract('src', ['qaiu'], type='float', alias='qaiu') }},
        {{ trx_json_extract('src', ['qapu'], type='float', alias='qapu') }},
        {{ trx_json_extract('src', ['logn'], type='varchar', alias='logn') }},
        {{ trx_json_extract('src', ['inmv'], type='int', alias='inmv') }},
        {{ trx_json_extract('src', ['inmv_kw'], type='varchar', alias='inmv_kw') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_loca_ref_compnr'], type='int', alias='cwar_loca_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['atse_ref_compnr'], type='int', alias='atse_ref_compnr') }},
        {{ trx_json_extract('src', ['bpid_ref_compnr'], type='int', alias='bpid_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinr100') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'item', 'clot', 'serl', 'idat', 'cwar', 'loca', 'trdt', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
