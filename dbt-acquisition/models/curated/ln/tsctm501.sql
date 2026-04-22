{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm501 Warranty Transactions table, Generated 2026-04-10T19:42:35Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['wrty'], type='varchar', alias='wrty') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['sern'], type='varchar', alias='sern') }},
        {{ trx_json_extract('src', ['trtm'], type='timestamp_ntz', alias='trtm') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['cctp'], type='varchar', alias='cctp') }},
        {{ trx_json_extract('src', ['cotp'], type='int', alias='cotp') }},
        {{ trx_json_extract('src', ['cotp_kw'], type='varchar', alias='cotp_kw') }},
        {{ trx_json_extract('src', ['ortp'], type='int', alias='ortp') }},
        {{ trx_json_extract('src', ['ortp_kw'], type='varchar', alias='ortp_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['cvln'], type='int', alias='cvln') }},
        {{ trx_json_extract('src', ['tcst'], type='int', alias='tcst') }},
        {{ trx_json_extract('src', ['tcst_kw'], type='varchar', alias='tcst_kw') }},
        {{ trx_json_extract('src', ['wrtp'], type='int', alias='wrtp') }},
        {{ trx_json_extract('src', ['wrtp_kw'], type='varchar', alias='wrtp_kw') }},
        {{ trx_json_extract('src', ['cstp'], type='varchar', alias='cstp') }},
        {{ trx_json_extract('src', ['cwoc'], type='varchar', alias='cwoc') }},
        {{ trx_json_extract('src', ['clst'], type='varchar', alias='clst') }},
        {{ trx_json_extract('src', ['litm'], type='varchar', alias='litm') }},
        {{ trx_json_extract('src', ['lser'], type='varchar', alias='lser') }},
        {{ trx_json_extract('src', ['ofbp'], type='varchar', alias='ofbp') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['spco_1'], type='float', alias='spco_1') }},
        {{ trx_json_extract('src', ['spco_2'], type='float', alias='spco_2') }},
        {{ trx_json_extract('src', ['spco_3'], type='float', alias='spco_3') }},
        {{ trx_json_extract('src', ['spsa'], type='float', alias='spsa') }},
        {{ trx_json_extract('src', ['item_sern_ref_compnr'], type='int', alias='item_sern_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cctp_ref_compnr'], type='int', alias='cctp_ref_compnr') }},
        {{ trx_json_extract('src', ['cstp_ref_compnr'], type='int', alias='cstp_ref_compnr') }},
        {{ trx_json_extract('src', ['cwoc_ref_compnr'], type='int', alias='cwoc_ref_compnr') }},
        {{ trx_json_extract('src', ['clst_ref_compnr'], type='int', alias='clst_ref_compnr') }},
        {{ trx_json_extract('src', ['litm_lser_ref_compnr'], type='int', alias='litm_lser_ref_compnr') }},
        {{ trx_json_extract('src', ['litm_ref_compnr'], type='int', alias='litm_ref_compnr') }},
        {{ trx_json_extract('src', ['ofbp_ref_compnr'], type='int', alias='ofbp_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['spco_trnc'], type='float', alias='spco_trnc') }},
        {{ trx_json_extract('src', ['spco_refc'], type='float', alias='spco_refc') }},
        {{ trx_json_extract('src', ['spco_dwhc'], type='float', alias='spco_dwhc') }},
        {{ trx_json_extract('src', ['spsa_homc'], type='float', alias='spsa_homc') }},
        {{ trx_json_extract('src', ['spsa_rpc1'], type='float', alias='spsa_rpc1') }},
        {{ trx_json_extract('src', ['spsa_rpc2'], type='float', alias='spsa_rpc2') }},
        {{ trx_json_extract('src', ['spsa_refc'], type='float', alias='spsa_refc') }},
        {{ trx_json_extract('src', ['spsa_dwhc'], type='float', alias='spsa_dwhc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm501') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'wrty', 'item', 'sern', 'trtm', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
