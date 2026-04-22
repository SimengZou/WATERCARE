{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whina126 Inventory Receipt Transaction Variances table, Generated 2026-04-10T19:42:46Z from package combination ce01101',
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
        {{ trx_json_extract('src', ['vpdt'], type='timestamp_ntz', alias='vpdt') }},
        {{ trx_json_extract('src', ['vseq'], type='int', alias='vseq') }},
        {{ trx_json_extract('src', ['lgdt'], type='timestamp_ntz', alias='lgdt') }},
        {{ trx_json_extract('src', ['reva'], type='int', alias='reva') }},
        {{ trx_json_extract('src', ['reva_kw'], type='varchar', alias='reva_kw') }},
        {{ trx_json_extract('src', ['iror'], type='int', alias='iror') }},
        {{ trx_json_extract('src', ['iror_kw'], type='varchar', alias='iror_kw') }},
        {{ trx_json_extract('src', ['ivsq'], type='int', alias='ivsq') }},
        {{ trx_json_extract('src', ['rorg'], type='int', alias='rorg') }},
        {{ trx_json_extract('src', ['rorg_kw'], type='varchar', alias='rorg_kw') }},
        {{ trx_json_extract('src', ['rorn'], type='varchar', alias='rorn') }},
        {{ trx_json_extract('src', ['rseq'], type='int', alias='rseq') }},
        {{ trx_json_extract('src', ['item_cwar_trdt_seqn_inwp_ref_compnr'], type='int', alias='item_cwar_trdt_seqn_inwp_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whina126') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'item', 'cwar', 'trdt', 'seqn', 'inwp', 'vpdt', 'vseq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
