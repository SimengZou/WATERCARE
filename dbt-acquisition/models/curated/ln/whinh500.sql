{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinh500 Cycle Counting Orders table, Generated 2026-04-10T19:42:51Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['cntn'], type='int', alias='cntn') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['rcyn'], type='int', alias='rcyn') }},
        {{ trx_json_extract('src', ['rcyn_kw'], type='varchar', alias='rcyn_kw') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['odat'], type='timestamp_ntz', alias='odat') }},
        {{ trx_json_extract('src', ['prnt'], type='int', alias='prnt') }},
        {{ trx_json_extract('src', ['prnt_kw'], type='varchar', alias='prnt_kw') }},
        {{ trx_json_extract('src', ['prcc'], type='int', alias='prcc') }},
        {{ trx_json_extract('src', ['prcc_kw'], type='varchar', alias='prcc_kw') }},
        {{ trx_json_extract('src', ['ccst'], type='int', alias='ccst') }},
        {{ trx_json_extract('src', ['ccst_kw'], type='varchar', alias='ccst_kw') }},
        {{ trx_json_extract('src', ['refe', 'en_US'], type='varchar', alias='refe__en_us') }},
        {{ trx_json_extract('src', ['redt'], type='timestamp_ntz', alias='redt') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cdf_adat'], type='timestamp_ntz', alias='cdf_adat') }},
        {{ trx_json_extract('src', ['cdf_ausr'], type='varchar', alias='cdf_ausr') }},
        {{ trx_json_extract('src', ['cdf_rdte'], type='timestamp_ntz', alias='cdf_rdte') }},
        {{ trx_json_extract('src', ['cdf_rfin'], type='int', alias='cdf_rfin') }},
        {{ trx_json_extract('src', ['cdf_rfin_kw'], type='varchar', alias='cdf_rfin_kw') }},
        {{ trx_json_extract('src', ['cdf_rusr'], type='varchar', alias='cdf_rusr') }},
        {{ trx_json_extract('src', ['cdf_wfst'], type='int', alias='cdf_wfst') }},
        {{ trx_json_extract('src', ['cdf_wfst_kw'], type='varchar', alias='cdf_wfst_kw') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinh500') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'orno', 'cntn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
