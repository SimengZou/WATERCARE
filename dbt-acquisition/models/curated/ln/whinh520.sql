{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinh520 Adjustment Orders table, Generated 2026-04-10T19:42:51Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['odat'], type='timestamp_ntz', alias='odat') }},
        {{ trx_json_extract('src', ['adrn'], type='varchar', alias='adrn') }},
        {{ trx_json_extract('src', ['mnad'], type='int', alias='mnad') }},
        {{ trx_json_extract('src', ['mnad_kw'], type='varchar', alias='mnad_kw') }},
        {{ trx_json_extract('src', ['adst'], type='int', alias='adst') }},
        {{ trx_json_extract('src', ['adst_kw'], type='varchar', alias='adst_kw') }},
        {{ trx_json_extract('src', ['refe', 'en_US'], type='varchar', alias='refe__en_us') }},
        {{ trx_json_extract('src', ['redt'], type='timestamp_ntz', alias='redt') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['adrn_ref_compnr'], type='int', alias='adrn_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinh520') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'orno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
