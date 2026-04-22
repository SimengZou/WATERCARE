{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tdsmi110 Opportunities table, Generated 2026-04-10T19:41:27Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['opty'], type='varchar', alias='opty') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['seri'], type='varchar', alias='seri') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['crep'], type='varchar', alias='crep') }},
        {{ trx_json_extract('src', ['cptp'], type='varchar', alias='cptp') }},
        {{ trx_json_extract('src', ['csou'], type='varchar', alias='csou') }},
        {{ trx_json_extract('src', ['opst'], type='int', alias='opst') }},
        {{ trx_json_extract('src', ['opst_kw'], type='varchar', alias='opst_kw') }},
        {{ trx_json_extract('src', ['sapr'], type='varchar', alias='sapr') }},
        {{ trx_json_extract('src', ['cpha'], type='varchar', alias='cpha') }},
        {{ trx_json_extract('src', ['sccp'], type='float', alias='sccp') }},
        {{ trx_json_extract('src', ['cdis'], type='varchar', alias='cdis') }},
        {{ trx_json_extract('src', ['incf'], type='int', alias='incf') }},
        {{ trx_json_extract('src', ['incf_kw'], type='varchar', alias='incf_kw') }},
        {{ trx_json_extract('src', ['xpam'], type='float', alias='xpam') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['fcdt'], type='timestamp_ntz', alias='fcdt') }},
        {{ trx_json_extract('src', ['dcdt'], type='timestamp_ntz', alias='dcdt') }},
        {{ trx_json_extract('src', ['acdt'], type='timestamp_ntz', alias='acdt') }},
        {{ trx_json_extract('src', ['cofc'], type='varchar', alias='cofc') }},
        {{ trx_json_extract('src', ['catt'], type='varchar', alias='catt') }},
        {{ trx_json_extract('src', ['ccor'], type='varchar', alias='ccor') }},
        {{ trx_json_extract('src', ['telp'], type='varchar', alias='telp') }},
        {{ trx_json_extract('src', ['tefx'], type='varchar', alias='tefx') }},
        {{ trx_json_extract('src', ['guid'], type='varchar', alias='guid') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['lmus'], type='varchar', alias='lmus') }},
        {{ trx_json_extract('src', ['ltdt'], type='timestamp_ntz', alias='ltdt') }},
        {{ trx_json_extract('src', ['name', 'en_US'], type='varchar', alias='name__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['mprj'], type='varchar', alias='mprj') }},
        {{ trx_json_extract('src', ['role'], type='int', alias='role') }},
        {{ trx_json_extract('src', ['role_kw'], type='varchar', alias='role_kw') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['bpid_ref_compnr'], type='int', alias='bpid_ref_compnr') }},
        {{ trx_json_extract('src', ['crep_ref_compnr'], type='int', alias='crep_ref_compnr') }},
        {{ trx_json_extract('src', ['cptp_ref_compnr'], type='int', alias='cptp_ref_compnr') }},
        {{ trx_json_extract('src', ['csou_ref_compnr'], type='int', alias='csou_ref_compnr') }},
        {{ trx_json_extract('src', ['sapr_cpha_ref_compnr'], type='int', alias='sapr_cpha_ref_compnr') }},
        {{ trx_json_extract('src', ['sapr_ref_compnr'], type='int', alias='sapr_ref_compnr') }},
        {{ trx_json_extract('src', ['cpha_ref_compnr'], type='int', alias='cpha_ref_compnr') }},
        {{ trx_json_extract('src', ['cdis_ref_compnr'], type='int', alias='cdis_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['cofc_ref_compnr'], type='int', alias='cofc_ref_compnr') }},
        {{ trx_json_extract('src', ['catt_ref_compnr'], type='int', alias='catt_ref_compnr') }},
        {{ trx_json_extract('src', ['ccor_ref_compnr'], type='int', alias='ccor_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['xpam_lclc'], type='float', alias='xpam_lclc') }},
        {{ trx_json_extract('src', ['xpam_rpc1'], type='float', alias='xpam_rpc1') }},
        {{ trx_json_extract('src', ['xpam_rpc2'], type='float', alias='xpam_rpc2') }},
        {{ trx_json_extract('src', ['xpam_rfrc'], type='float', alias='xpam_rfrc') }},
        {{ trx_json_extract('src', ['xpam_dtwc'], type='float', alias='xpam_dtwc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tdsmi110') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'opty']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
