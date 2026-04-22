{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinh600 Cross-dock Orders table, Generated 2026-04-10T19:42:52Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cdor'], type='varchar', alias='cdor') }},
        {{ trx_json_extract('src', ['oorg'], type='int', alias='oorg') }},
        {{ trx_json_extract('src', ['oorg_kw'], type='varchar', alias='oorg_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['oset'], type='int', alias='oset') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['boml'], type='int', alias='boml') }},
        {{ trx_json_extract('src', ['dlin'], type='int', alias='dlin') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['stlo'], type='varchar', alias='stlo') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['cdty'], type='int', alias='cdty') }},
        {{ trx_json_extract('src', ['cdty_kw'], type='varchar', alias='cdty_kw') }},
        {{ trx_json_extract('src', ['uspr'], type='int', alias='uspr') }},
        {{ trx_json_extract('src', ['sypr'], type='varchar', alias='sypr') }},
        {{ trx_json_extract('src', ['cdpd'], type='varchar', alias='cdpd') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['lcdt'], type='timestamp_ntz', alias='lcdt') }},
        {{ trx_json_extract('src', ['pddt'], type='timestamp_ntz', alias='pddt') }},
        {{ trx_json_extract('src', ['qrun'], type='float', alias='qrun') }},
        {{ trx_json_extract('src', ['cdun'], type='varchar', alias='cdun') }},
        {{ trx_json_extract('src', ['qreq'], type='float', alias='qreq') }},
        {{ trx_json_extract('src', ['qpla'], type='float', alias='qpla') }},
        {{ trx_json_extract('src', ['qapp'], type='float', alias='qapp') }},
        {{ trx_json_extract('src', ['qadv'], type='float', alias='qadv') }},
        {{ trx_json_extract('src', ['qact'], type='float', alias='qact') }},
        {{ trx_json_extract('src', ['cdos'], type='int', alias='cdos') }},
        {{ trx_json_extract('src', ['cdos_kw'], type='varchar', alias='cdos_kw') }},
        {{ trx_json_extract('src', ['cwar_stlo_ref_compnr'], type='int', alias='cwar_stlo_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cdpd_ref_compnr'], type='int', alias='cdpd_ref_compnr') }},
        {{ trx_json_extract('src', ['cdun_ref_compnr'], type='int', alias='cdun_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinh600') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cdor']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
