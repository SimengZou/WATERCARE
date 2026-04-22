{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tdpur200 Purchase Requisitions table, Generated 2026-04-10T19:41:19Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['rqno'], type='varchar', alias='rqno') }},
        {{ trx_json_extract('src', ['orig'], type='int', alias='orig') }},
        {{ trx_json_extract('src', ['orig_kw'], type='varchar', alias='orig_kw') }},
        {{ trx_json_extract('src', ['remn'], type='varchar', alias='remn') }},
        {{ trx_json_extract('src', ['rdep'], type='varchar', alias='rdep') }},
        {{ trx_json_extract('src', ['cofc'], type='varchar', alias='cofc') }},
        {{ trx_json_extract('src', ['rdat'], type='timestamp_ntz', alias='rdat') }},
        {{ trx_json_extract('src', ['aemn'], type='varchar', alias='aemn') }},
        {{ trx_json_extract('src', ['adep'], type='varchar', alias='adep') }},
        {{ trx_json_extract('src', ['ltdt'], type='timestamp_ntz', alias='ltdt') }},
        {{ trx_json_extract('src', ['rqst'], type='int', alias='rqst') }},
        {{ trx_json_extract('src', ['rqst_kw'], type='varchar', alias='rqst_kw') }},
        {{ trx_json_extract('src', ['conv'], type='int', alias='conv') }},
        {{ trx_json_extract('src', ['conv_kw'], type='varchar', alias='conv_kw') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['dadr'], type='varchar', alias='dadr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['cstl'], type='varchar', alias='cstl') }},
        {{ trx_json_extract('src', ['ccco'], type='varchar', alias='ccco') }},
        {{ trx_json_extract('src', ['dldt'], type='timestamp_ntz', alias='dldt') }},
        {{ trx_json_extract('src', ['refa', 'en_US'], type='varchar', alias='refa__en_us') }},
        {{ trx_json_extract('src', ['refb', 'en_US'], type='varchar', alias='refb__en_us') }},
        {{ trx_json_extract('src', ['logn'], type='varchar', alias='logn') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['ccon'], type='varchar', alias='ccon') }},
        {{ trx_json_extract('src', ['urgt'], type='int', alias='urgt') }},
        {{ trx_json_extract('src', ['urgt_kw'], type='varchar', alias='urgt_kw') }},
        {{ trx_json_extract('src', ['cnty'], type='int', alias='cnty') }},
        {{ trx_json_extract('src', ['cnty_kw'], type='varchar', alias='cnty_kw') }},
        {{ trx_json_extract('src', ['spap'], type='int', alias='spap') }},
        {{ trx_json_extract('src', ['spap_kw'], type='varchar', alias='spap_kw') }},
        {{ trx_json_extract('src', ['rcod'], type='varchar', alias='rcod') }},
        {{ trx_json_extract('src', ['dral'], type='int', alias='dral') }},
        {{ trx_json_extract('src', ['dral_kw'], type='varchar', alias='dral_kw') }},
        {{ trx_json_extract('src', ['adin'], type='varchar', alias='adin') }},
        {{ trx_json_extract('src', ['ccty'], type='varchar', alias='ccty') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['remn_ref_compnr'], type='int', alias='remn_ref_compnr') }},
        {{ trx_json_extract('src', ['rdep_ref_compnr'], type='int', alias='rdep_ref_compnr') }},
        {{ trx_json_extract('src', ['cofc_ref_compnr'], type='int', alias='cofc_ref_compnr') }},
        {{ trx_json_extract('src', ['aemn_ref_compnr'], type='int', alias='aemn_ref_compnr') }},
        {{ trx_json_extract('src', ['adep_ref_compnr'], type='int', alias='adep_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['dadr_ref_compnr'], type='int', alias='dadr_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ccco_ref_compnr'], type='int', alias='ccco_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['ccon_ref_compnr'], type='int', alias='ccon_ref_compnr') }},
        {{ trx_json_extract('src', ['rcod_ref_compnr'], type='int', alias='rcod_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tdpur200') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'rqno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
