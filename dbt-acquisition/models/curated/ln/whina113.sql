{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whina113 Inventory Receipt Transaction - Cost Details table, Generated 2026-04-10T19:42:44Z from package combination ce01101',
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
        {{ trx_json_extract('src', ['cpcp'], type='varchar', alias='cpcp') }},
        {{ trx_json_extract('src', ['hour'], type='float', alias='hour') }},
        {{ trx_json_extract('src', ['mauh'], type='float', alias='mauh') }},
        {{ trx_json_extract('src', ['math'], type='float', alias='math') }},
        {{ trx_json_extract('src', ['amah'], type='float', alias='amah') }},
        {{ trx_json_extract('src', ['atmh'], type='float', alias='atmh') }},
        {{ trx_json_extract('src', ['amnt_1'], type='float', alias='amnt_1') }},
        {{ trx_json_extract('src', ['amnt_2'], type='float', alias='amnt_2') }},
        {{ trx_json_extract('src', ['amnt_3'], type='float', alias='amnt_3') }},
        {{ trx_json_extract('src', ['mauc_1'], type='float', alias='mauc_1') }},
        {{ trx_json_extract('src', ['mauc_2'], type='float', alias='mauc_2') }},
        {{ trx_json_extract('src', ['mauc_3'], type='float', alias='mauc_3') }},
        {{ trx_json_extract('src', ['matc_1'], type='float', alias='matc_1') }},
        {{ trx_json_extract('src', ['matc_2'], type='float', alias='matc_2') }},
        {{ trx_json_extract('src', ['matc_3'], type='float', alias='matc_3') }},
        {{ trx_json_extract('src', ['amac_1'], type='float', alias='amac_1') }},
        {{ trx_json_extract('src', ['amac_2'], type='float', alias='amac_2') }},
        {{ trx_json_extract('src', ['amac_3'], type='float', alias='amac_3') }},
        {{ trx_json_extract('src', ['atmc_1'], type='float', alias='atmc_1') }},
        {{ trx_json_extract('src', ['atmc_2'], type='float', alias='atmc_2') }},
        {{ trx_json_extract('src', ['atmc_3'], type='float', alias='atmc_3') }},
        {{ trx_json_extract('src', ['item_cwar_trdt_seqn_inwp_ref_compnr'], type='int', alias='item_cwar_trdt_seqn_inwp_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcp_ref_compnr'], type='int', alias='cpcp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whina113') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'item', 'cwar', 'trdt', 'seqn', 'inwp', 'cpcp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
