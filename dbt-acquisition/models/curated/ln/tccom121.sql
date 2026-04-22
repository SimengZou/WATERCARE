{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tccom121 Ship-from Business Partners table, Generated 2026-04-10T19:41:03Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['sfbp'], type='varchar', alias='sfbp') }},
        {{ trx_json_extract('src', ['bpst'], type='int', alias='bpst') }},
        {{ trx_json_extract('src', ['bpst_kw'], type='varchar', alias='bpst_kw') }},
        {{ trx_json_extract('src', ['stdt'], type='timestamp_ntz', alias='stdt') }},
        {{ trx_json_extract('src', ['endt'], type='timestamp_ntz', alias='endt') }},
        {{ trx_json_extract('src', ['clan'], type='varchar', alias='clan') }},
        {{ trx_json_extract('src', ['cbps'], type='varchar', alias='cbps') }},
        {{ trx_json_extract('src', ['cbtp'], type='varchar', alias='cbtp') }},
        {{ trx_json_extract('src', ['vryn'], type='int', alias='vryn') }},
        {{ trx_json_extract('src', ['vryn_kw'], type='varchar', alias='vryn_kw') }},
        {{ trx_json_extract('src', ['cadr'], type='varchar', alias='cadr') }},
        {{ trx_json_extract('src', ['telp'], type='varchar', alias='telp') }},
        {{ trx_json_extract('src', ['tlcy'], type='varchar', alias='tlcy') }},
        {{ trx_json_extract('src', ['tlci'], type='varchar', alias='tlci') }},
        {{ trx_json_extract('src', ['tlln'], type='varchar', alias='tlln') }},
        {{ trx_json_extract('src', ['tlex'], type='varchar', alias='tlex') }},
        {{ trx_json_extract('src', ['tefx'], type='varchar', alias='tefx') }},
        {{ trx_json_extract('src', ['tfcy'], type='varchar', alias='tfcy') }},
        {{ trx_json_extract('src', ['tfci'], type='varchar', alias='tfci') }},
        {{ trx_json_extract('src', ['tfln'], type='varchar', alias='tfln') }},
        {{ trx_json_extract('src', ['tfex'], type='varchar', alias='tfex') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['ccnt'], type='varchar', alias='ccnt') }},
        {{ trx_json_extract('src', ['serv'], type='varchar', alias='serv') }},
        {{ trx_json_extract('src', ['otbp'], type='varchar', alias='otbp') }},
        {{ trx_json_extract('src', ['cfrw'], type='varchar', alias='cfrw') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['qual'], type='int', alias='qual') }},
        {{ trx_json_extract('src', ['qual_kw'], type='varchar', alias='qual_kw') }},
        {{ trx_json_extract('src', ['sasn'], type='int', alias='sasn') }},
        {{ trx_json_extract('src', ['sasn_kw'], type='varchar', alias='sasn_kw') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['cdec'], type='varchar', alias='cdec') }},
        {{ trx_json_extract('src', ['ptpa'], type='varchar', alias='ptpa') }},
        {{ trx_json_extract('src', ['rdec'], type='varchar', alias='rdec') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['lmus'], type='varchar', alias='lmus') }},
        {{ trx_json_extract('src', ['lmdt'], type='timestamp_ntz', alias='lmdt') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['sfbp_ref_compnr'], type='int', alias='sfbp_ref_compnr') }},
        {{ trx_json_extract('src', ['clan_ref_compnr'], type='int', alias='clan_ref_compnr') }},
        {{ trx_json_extract('src', ['cbps_ref_compnr'], type='int', alias='cbps_ref_compnr') }},
        {{ trx_json_extract('src', ['cbtp_ref_compnr'], type='int', alias='cbtp_ref_compnr') }},
        {{ trx_json_extract('src', ['cadr_ref_compnr'], type='int', alias='cadr_ref_compnr') }},
        {{ trx_json_extract('src', ['ccal_ref_compnr'], type='int', alias='ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['ccnt_ref_compnr'], type='int', alias='ccnt_ref_compnr') }},
        {{ trx_json_extract('src', ['serv_ref_compnr'], type='int', alias='serv_ref_compnr') }},
        {{ trx_json_extract('src', ['otbp_ref_compnr'], type='int', alias='otbp_ref_compnr') }},
        {{ trx_json_extract('src', ['cfrw_ref_compnr'], type='int', alias='cfrw_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['cdec_ref_compnr'], type='int', alias='cdec_ref_compnr') }},
        {{ trx_json_extract('src', ['ptpa_ref_compnr'], type='int', alias='ptpa_ref_compnr') }},
        {{ trx_json_extract('src', ['rdec_ref_compnr'], type='int', alias='rdec_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tccom121') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'sfbp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
