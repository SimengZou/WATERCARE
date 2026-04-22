{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinh435 Delivery Notes table, Generated 2026-04-10T19:42:50Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['prdn'], type='varchar', alias='prdn') }},
        {{ trx_json_extract('src', ['deln'], type='varchar', alias='deln') }},
        {{ trx_json_extract('src', ['fsyr'], type='int', alias='fsyr') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['load'], type='varchar', alias='load') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['sfty'], type='int', alias='sfty') }},
        {{ trx_json_extract('src', ['sfty_kw'], type='varchar', alias='sfty_kw') }},
        {{ trx_json_extract('src', ['sfco'], type='varchar', alias='sfco') }},
        {{ trx_json_extract('src', ['sfad'], type='varchar', alias='sfad') }},
        {{ trx_json_extract('src', ['sfcp'], type='int', alias='sfcp') }},
        {{ trx_json_extract('src', ['stty'], type='int', alias='stty') }},
        {{ trx_json_extract('src', ['stty_kw'], type='varchar', alias='stty_kw') }},
        {{ trx_json_extract('src', ['stco'], type='varchar', alias='stco') }},
        {{ trx_json_extract('src', ['stad'], type='varchar', alias='stad') }},
        {{ trx_json_extract('src', ['stcp'], type='int', alias='stcp') }},
        {{ trx_json_extract('src', ['tsad'], type='varchar', alias='tsad') }},
        {{ trx_json_extract('src', ['depc'], type='int', alias='depc') }},
        {{ trx_json_extract('src', ['wdep'], type='varchar', alias='wdep') }},
        {{ trx_json_extract('src', ['itbp'], type='varchar', alias='itbp') }},
        {{ trx_json_extract('src', ['ofbp'], type='varchar', alias='ofbp') }},
        {{ trx_json_extract('src', ['ccty'], type='varchar', alias='ccty') }},
        {{ trx_json_extract('src', ['bptc'], type='varchar', alias='bptc') }},
        {{ trx_json_extract('src', ['crte'], type='varchar', alias='crte') }},
        {{ trx_json_extract('src', ['cdec'], type='varchar', alias='cdec') }},
        {{ trx_json_extract('src', ['dnst'], type='int', alias='dnst') }},
        {{ trx_json_extract('src', ['dnst_kw'], type='varchar', alias='dnst_kw') }},
        {{ trx_json_extract('src', ['finp'], type='int', alias='finp') }},
        {{ trx_json_extract('src', ['finp_kw'], type='varchar', alias='finp_kw') }},
        {{ trx_json_extract('src', ['prdt'], type='timestamp_ntz', alias='prdt') }},
        {{ trx_json_extract('src', ['dcdt'], type='timestamp_ntz', alias='dcdt') }},
        {{ trx_json_extract('src', ['manl'], type='int', alias='manl') }},
        {{ trx_json_extract('src', ['manl_kw'], type='varchar', alias='manl_kw') }},
        {{ trx_json_extract('src', ['motv'], type='varchar', alias='motv') }},
        {{ trx_json_extract('src', ['delc'], type='varchar', alias='delc') }},
        {{ trx_json_extract('src', ['taxs'], type='varchar', alias='taxs') }},
        {{ trx_json_extract('src', ['taxp'], type='varchar', alias='taxp') }},
        {{ trx_json_extract('src', ['qpac'], type='float', alias='qpac') }},
        {{ trx_json_extract('src', ['shap', 'en_US'], type='varchar', alias='shap__en_us') }},
        {{ trx_json_extract('src', ['nota', 'en_US'], type='varchar', alias='nota__en_us') }},
        {{ trx_json_extract('src', ['notb', 'en_US'], type='varchar', alias='notb__en_us') }},
        {{ trx_json_extract('src', ['notc', 'en_US'], type='varchar', alias='notc__en_us') }},
        {{ trx_json_extract('src', ['grwt'], type='float', alias='grwt') }},
        {{ trx_json_extract('src', ['cwun'], type='varchar', alias='cwun') }},
        {{ trx_json_extract('src', ['carp'], type='varchar', alias='carp') }},
        {{ trx_json_extract('src', ['fnsr'], type='varchar', alias='fnsr') }},
        {{ trx_json_extract('src', ['load_ref_compnr'], type='int', alias='load_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['sfad_ref_compnr'], type='int', alias='sfad_ref_compnr') }},
        {{ trx_json_extract('src', ['stad_ref_compnr'], type='int', alias='stad_ref_compnr') }},
        {{ trx_json_extract('src', ['tsad_ref_compnr'], type='int', alias='tsad_ref_compnr') }},
        {{ trx_json_extract('src', ['depc_ref_compnr'], type='int', alias='depc_ref_compnr') }},
        {{ trx_json_extract('src', ['itbp_ref_compnr'], type='int', alias='itbp_ref_compnr') }},
        {{ trx_json_extract('src', ['ofbp_ref_compnr'], type='int', alias='ofbp_ref_compnr') }},
        {{ trx_json_extract('src', ['ccty_ref_compnr'], type='int', alias='ccty_ref_compnr') }},
        {{ trx_json_extract('src', ['bptc_ref_compnr'], type='int', alias='bptc_ref_compnr') }},
        {{ trx_json_extract('src', ['crte_ref_compnr'], type='int', alias='crte_ref_compnr') }},
        {{ trx_json_extract('src', ['cdec_ref_compnr'], type='int', alias='cdec_ref_compnr') }},
        {{ trx_json_extract('src', ['motv_ref_compnr'], type='int', alias='motv_ref_compnr') }},
        {{ trx_json_extract('src', ['delc_ref_compnr'], type='int', alias='delc_ref_compnr') }},
        {{ trx_json_extract('src', ['cwun_ref_compnr'], type='int', alias='cwun_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinh435') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'prdn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
