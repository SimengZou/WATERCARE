{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tdpur100 Requests for Quotation table, Generated 2026-04-10T19:41:18Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['qono'], type='varchar', alias='qono') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['rfqt'], type='varchar', alias='rfqt') }},
        {{ trx_json_extract('src', ['negt'], type='int', alias='negt') }},
        {{ trx_json_extract('src', ['negt_kw'], type='varchar', alias='negt_kw') }},
        {{ trx_json_extract('src', ['cneg'], type='int', alias='cneg') }},
        {{ trx_json_extract('src', ['cneg_kw'], type='varchar', alias='cneg_kw') }},
        {{ trx_json_extract('src', ['cpay'], type='varchar', alias='cpay') }},
        {{ trx_json_extract('src', ['qdat'], type='timestamp_ntz', alias='qdat') }},
        {{ trx_json_extract('src', ['cdec'], type='varchar', alias='cdec') }},
        {{ trx_json_extract('src', ['ptpa'], type='varchar', alias='ptpa') }},
        {{ trx_json_extract('src', ['qspa'], type='int', alias='qspa') }},
        {{ trx_json_extract('src', ['qspa_kw'], type='varchar', alias='qspa_kw') }},
        {{ trx_json_extract('src', ['lcmp'], type='int', alias='lcmp') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['cadr'], type='varchar', alias='cadr') }},
        {{ trx_json_extract('src', ['refa', 'en_US'], type='varchar', alias='refa__en_us') }},
        {{ trx_json_extract('src', ['refb', 'en_US'], type='varchar', alias='refb__en_us') }},
        {{ trx_json_extract('src', ['rtdt'], type='timestamp_ntz', alias='rtdt') }},
        {{ trx_json_extract('src', ['ccon'], type='varchar', alias='ccon') }},
        {{ trx_json_extract('src', ['ddat'], type='timestamp_ntz', alias='ddat') }},
        {{ trx_json_extract('src', ['sdat'], type='timestamp_ntz', alias='sdat') }},
        {{ trx_json_extract('src', ['edat'], type='timestamp_ntz', alias='edat') }},
        {{ trx_json_extract('src', ['rdat'], type='timestamp_ntz', alias='rdat') }},
        {{ trx_json_extract('src', ['cofc'], type='varchar', alias='cofc') }},
        {{ trx_json_extract('src', ['cset'], type='varchar', alias='cset') }},
        {{ trx_json_extract('src', ['ulcr'], type='int', alias='ulcr') }},
        {{ trx_json_extract('src', ['ulcr_kw'], type='varchar', alias='ulcr_kw') }},
        {{ trx_json_extract('src', ['uptc'], type='int', alias='uptc') }},
        {{ trx_json_extract('src', ['uptc_kw'], type='varchar', alias='uptc_kw') }},
        {{ trx_json_extract('src', ['adin'], type='varchar', alias='adin') }},
        {{ trx_json_extract('src', ['crcd'], type='varchar', alias='crcd') }},
        {{ trx_json_extract('src', ['ctcd'], type='varchar', alias='ctcd') }},
        {{ trx_json_extract('src', ['enlh'], type='int', alias='enlh') }},
        {{ trx_json_extract('src', ['enlh_kw'], type='varchar', alias='enlh_kw') }},
        {{ trx_json_extract('src', ['etpc'], type='int', alias='etpc') }},
        {{ trx_json_extract('src', ['etpc_kw'], type='varchar', alias='etpc_kw') }},
        {{ trx_json_extract('src', ['tpbk'], type='varchar', alias='tpbk') }},
        {{ trx_json_extract('src', ['cotp'], type='varchar', alias='cotp') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['txtb'], type='int', alias='txtb') }},
        {{ trx_json_extract('src', ['rfqt_ref_compnr'], type='int', alias='rfqt_ref_compnr') }},
        {{ trx_json_extract('src', ['cpay_ref_compnr'], type='int', alias='cpay_ref_compnr') }},
        {{ trx_json_extract('src', ['cdec_ref_compnr'], type='int', alias='cdec_ref_compnr') }},
        {{ trx_json_extract('src', ['ptpa_ref_compnr'], type='int', alias='ptpa_ref_compnr') }},
        {{ trx_json_extract('src', ['lcmp_ref_compnr'], type='int', alias='lcmp_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['cadr_ref_compnr'], type='int', alias='cadr_ref_compnr') }},
        {{ trx_json_extract('src', ['ccon_ref_compnr'], type='int', alias='ccon_ref_compnr') }},
        {{ trx_json_extract('src', ['cofc_ref_compnr'], type='int', alias='cofc_ref_compnr') }},
        {{ trx_json_extract('src', ['cset_ref_compnr'], type='int', alias='cset_ref_compnr') }},
        {{ trx_json_extract('src', ['crcd_ref_compnr'], type='int', alias='crcd_ref_compnr') }},
        {{ trx_json_extract('src', ['ctcd_ref_compnr'], type='int', alias='ctcd_ref_compnr') }},
        {{ trx_json_extract('src', ['tpbk_ref_compnr'], type='int', alias='tpbk_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['txtb_ref_compnr'], type='int', alias='txtb_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tdpur100') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'qono']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
