{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinr140 Inventory table, Generated 2025-06-12T01:48:44Z from package combination ce01090',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['loca'], type='varchar', alias='loca') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['clot'], type='varchar', alias='clot') }},
        {{ trx_json_extract('src', ['idat'], type='timestamp_ntz', alias='idat') }},
        {{ trx_json_extract('src', ['effn'], type='int', alias='effn') }},
        {{ trx_json_extract('src', ['qhnd'], type='float', alias='qhnd') }},
        {{ trx_json_extract('src', ['qblk'], type='float', alias='qblk') }},
        {{ trx_json_extract('src', ['qlal'], type='float', alias='qlal') }},
        {{ trx_json_extract('src', ['qord'], type='float', alias='qord') }},
        {{ trx_json_extract('src', ['qcom'], type='float', alias='qcom') }},
        {{ trx_json_extract('src', ['qcsl'], type='float', alias='qcsl') }},
        {{ trx_json_extract('src', ['ball'], type='int', alias='ball') }},
        {{ trx_json_extract('src', ['ball_kw'], type='varchar', alias='ball_kw') }},
        {{ trx_json_extract('src', ['bout'], type='int', alias='bout') }},
        {{ trx_json_extract('src', ['bout_kw'], type='varchar', alias='bout_kw') }},
        {{ trx_json_extract('src', ['btri'], type='int', alias='btri') }},
        {{ trx_json_extract('src', ['btri_kw'], type='varchar', alias='btri_kw') }},
        {{ trx_json_extract('src', ['bcyc'], type='int', alias='bcyc') }},
        {{ trx_json_extract('src', ['bcyc_kw'], type='varchar', alias='bcyc_kw') }},
        {{ trx_json_extract('src', ['bass'], type='int', alias='bass') }},
        {{ trx_json_extract('src', ['bass_kw'], type='varchar', alias='bass_kw') }},
        {{ trx_json_extract('src', ['crem'], type='varchar', alias='crem') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['trdt'], type='timestamp_ntz', alias='trdt') }},
        {{ trx_json_extract('src', ['lsid'], type='timestamp_ntz', alias='lsid') }},
        {{ trx_json_extract('src', ['mpkd'], type='int', alias='mpkd') }},
        {{ trx_json_extract('src', ['mpkd_kw'], type='varchar', alias='mpkd_kw') }},
        {{ trx_json_extract('src', ['cwar_loca_ref_compnr'], type='int', alias='cwar_loca_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['effn_ref_compnr'], type='int', alias='effn_ref_compnr') }},
        {{ trx_json_extract('src', ['crem_ref_compnr'], type='int', alias='crem_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinr140') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cwar', 'loca', 'item', 'clot', 'idat']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
