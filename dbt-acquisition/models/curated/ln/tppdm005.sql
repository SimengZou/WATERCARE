{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppdm005 Item Project Data table, Generated 2026-04-10T19:41:53Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['ccit'], type='varchar', alias='ccit') }},
        {{ trx_json_extract('src', ['ccfu'], type='int', alias='ccfu') }},
        {{ trx_json_extract('src', ['ccfu_kw'], type='varchar', alias='ccfu_kw') }},
        {{ trx_json_extract('src', ['prre'], type='int', alias='prre') }},
        {{ trx_json_extract('src', ['prre_kw'], type='varchar', alias='prre_kw') }},
        {{ trx_json_extract('src', ['osys'], type='int', alias='osys') }},
        {{ trx_json_extract('src', ['osys_kw'], type='varchar', alias='osys_kw') }},
        {{ trx_json_extract('src', ['pgwh'], type='int', alias='pgwh') }},
        {{ trx_json_extract('src', ['pgwh_kw'], type='varchar', alias='pgwh_kw') }},
        {{ trx_json_extract('src', ['copt'], type='int', alias='copt') }},
        {{ trx_json_extract('src', ['copt_kw'], type='varchar', alias='copt_kw') }},
        {{ trx_json_extract('src', ['cprp'], type='float', alias='cprp') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['ltcp'], type='timestamp_ntz', alias='ltcp') }},
        {{ trx_json_extract('src', ['cppp'], type='int', alias='cppp') }},
        {{ trx_json_extract('src', ['cppp_kw'], type='varchar', alias='cppp_kw') }},
        {{ trx_json_extract('src', ['cpmc_1'], type='float', alias='cpmc_1') }},
        {{ trx_json_extract('src', ['cpmc_2'], type='float', alias='cpmc_2') }},
        {{ trx_json_extract('src', ['cpmc_3'], type='float', alias='cpmc_3') }},
        {{ trx_json_extract('src', ['usyn'], type='int', alias='usyn') }},
        {{ trx_json_extract('src', ['usyn_kw'], type='varchar', alias='usyn_kw') }},
        {{ trx_json_extract('src', ['blbl'], type='int', alias='blbl') }},
        {{ trx_json_extract('src', ['blbl_kw'], type='varchar', alias='blbl_kw') }},
        {{ trx_json_extract('src', ['cuti'], type='varchar', alias='cuti') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['cuti_ref_compnr'], type='int', alias='cuti_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppdm005') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'item']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
