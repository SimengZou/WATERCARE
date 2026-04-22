{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tiipd001 Items - Production table, Generated 2026-04-10T19:41:47Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['cpha'], type='int', alias='cpha') }},
        {{ trx_json_extract('src', ['cpha_kw'], type='varchar', alias='cpha_kw') }},
        {{ trx_json_extract('src', ['oqdr'], type='int', alias='oqdr') }},
        {{ trx_json_extract('src', ['oqdr_kw'], type='varchar', alias='oqdr_kw') }},
        {{ trx_json_extract('src', ['phst'], type='int', alias='phst') }},
        {{ trx_json_extract('src', ['phst_kw'], type='varchar', alias='phst_kw') }},
        {{ trx_json_extract('src', ['iuma'], type='int', alias='iuma') }},
        {{ trx_json_extract('src', ['iuma_kw'], type='varchar', alias='iuma_kw') }},
        {{ trx_json_extract('src', ['repi'], type='int', alias='repi') }},
        {{ trx_json_extract('src', ['repi_kw'], type='varchar', alias='repi_kw') }},
        {{ trx_json_extract('src', ['scdl'], type='varchar', alias='scdl') }},
        {{ trx_json_extract('src', ['pcrp'], type='int', alias='pcrp') }},
        {{ trx_json_extract('src', ['bfcp'], type='int', alias='bfcp') }},
        {{ trx_json_extract('src', ['bfcp_kw'], type='varchar', alias='bfcp_kw') }},
        {{ trx_json_extract('src', ['bfep'], type='int', alias='bfep') }},
        {{ trx_json_extract('src', ['bfep_kw'], type='varchar', alias='bfep_kw') }},
        {{ trx_json_extract('src', ['bfhr'], type='int', alias='bfhr') }},
        {{ trx_json_extract('src', ['bfhr_kw'], type='varchar', alias='bfhr_kw') }},
        {{ trx_json_extract('src', ['unom'], type='float', alias='unom') }},
        {{ trx_json_extract('src', ['runi'], type='float', alias='runi') }},
        {{ trx_json_extract('src', ['scpf'], type='float', alias='scpf') }},
        {{ trx_json_extract('src', ['scpq'], type='float', alias='scpq') }},
        {{ trx_json_extract('src', ['drin'], type='int', alias='drin') }},
        {{ trx_json_extract('src', ['drin_kw'], type='varchar', alias='drin_kw') }},
        {{ trx_json_extract('src', ['dris'], type='int', alias='dris') }},
        {{ trx_json_extract('src', ['dris_kw'], type='varchar', alias='dris_kw') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['stoi'], type='int', alias='stoi') }},
        {{ trx_json_extract('src', ['stoi_kw'], type='varchar', alias='stoi_kw') }},
        {{ trx_json_extract('src', ['coac'], type='int', alias='coac') }},
        {{ trx_json_extract('src', ['coac_kw'], type='varchar', alias='coac_kw') }},
        {{ trx_json_extract('src', ['bomu'], type='int', alias='bomu') }},
        {{ trx_json_extract('src', ['bomu_kw'], type='varchar', alias='bomu_kw') }},
        {{ trx_json_extract('src', ['nsfc'], type='int', alias='nsfc') }},
        {{ trx_json_extract('src', ['nsfc_kw'], type='varchar', alias='nsfc_kw') }},
        {{ trx_json_extract('src', ['oltm'], type='float', alias='oltm') }},
        {{ trx_json_extract('src', ['oltu'], type='int', alias='oltu') }},
        {{ trx_json_extract('src', ['oltu_kw'], type='varchar', alias='oltu_kw') }},
        {{ trx_json_extract('src', ['swoc'], type='int', alias='swoc') }},
        {{ trx_json_extract('src', ['swoc_kw'], type='varchar', alias='swoc_kw') }},
        {{ trx_json_extract('src', ['cick'], type='int', alias='cick') }},
        {{ trx_json_extract('src', ['cick_kw'], type='varchar', alias='cick_kw') }},
        {{ trx_json_extract('src', ['smfs'], type='int', alias='smfs') }},
        {{ trx_json_extract('src', ['smfs_kw'], type='varchar', alias='smfs_kw') }},
        {{ trx_json_extract('src', ['iimf'], type='int', alias='iimf') }},
        {{ trx_json_extract('src', ['iimf_kw'], type='varchar', alias='iimf_kw') }},
        {{ trx_json_extract('src', ['iima'], type='int', alias='iima') }},
        {{ trx_json_extract('src', ['iima_kw'], type='varchar', alias='iima_kw') }},
        {{ trx_json_extract('src', ['cncd'], type='varchar', alias='cncd') }},
        {{ trx_json_extract('src', ['sfpl'], type='varchar', alias='sfpl') }},
        {{ trx_json_extract('src', ['rgrp'], type='varchar', alias='rgrp') }},
        {{ trx_json_extract('src', ['jsst'], type='varchar', alias='jsst') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['scdl_ref_compnr'], type='int', alias='scdl_ref_compnr') }},
        {{ trx_json_extract('src', ['sfpl_ref_compnr'], type='int', alias='sfpl_ref_compnr') }},
        {{ trx_json_extract('src', ['rgrp_ref_compnr'], type='int', alias='rgrp_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tiipd001') }}
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
