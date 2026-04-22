{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcemm170 Companies table, Generated 2026-04-10T19:41:07Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['comp'], type='int', alias='comp') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['ctyp'], type='int', alias='ctyp') }},
        {{ trx_json_extract('src', ['ctyp_kw'], type='varchar', alias='ctyp_kw') }},
        {{ trx_json_extract('src', ['csys'], type='int', alias='csys') }},
        {{ trx_json_extract('src', ['csys_kw'], type='varchar', alias='csys_kw') }},
        {{ trx_json_extract('src', ['fcua'], type='varchar', alias='fcua') }},
        {{ trx_json_extract('src', ['ctya'], type='int', alias='ctya') }},
        {{ trx_json_extract('src', ['ctya_kw'], type='varchar', alias='ctya_kw') }},
        {{ trx_json_extract('src', ['lcur'], type='varchar', alias='lcur') }},
        {{ trx_json_extract('src', ['fcub'], type='varchar', alias='fcub') }},
        {{ trx_json_extract('src', ['ctyb'], type='int', alias='ctyb') }},
        {{ trx_json_extract('src', ['ctyb_kw'], type='varchar', alias='ctyb_kw') }},
        {{ trx_json_extract('src', ['fcuc'], type='varchar', alias='fcuc') }},
        {{ trx_json_extract('src', ['ctyc'], type='int', alias='ctyc') }},
        {{ trx_json_extract('src', ['ctyc_kw'], type='varchar', alias='ctyc_kw') }},
        {{ trx_json_extract('src', ['umfc'], type='int', alias='umfc') }},
        {{ trx_json_extract('src', ['umfc_kw'], type='varchar', alias='umfc_kw') }},
        {{ trx_json_extract('src', ['clcu'], type='varchar', alias='clcu') }},
        {{ trx_json_extract('src', ['expu'], type='varchar', alias='expu') }},
        {{ trx_json_extract('src', ['exsa'], type='varchar', alias='exsa') }},
        {{ trx_json_extract('src', ['exeu'], type='varchar', alias='exeu') }},
        {{ trx_json_extract('src', ['exex'], type='varchar', alias='exex') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['tzid'], type='varchar', alias='tzid') }},
        {{ trx_json_extract('src', ['euro'], type='varchar', alias='euro') }},
        {{ trx_json_extract('src', ['tmub'], type='int', alias='tmub') }},
        {{ trx_json_extract('src', ['tmub_kw'], type='varchar', alias='tmub_kw') }},
        {{ trx_json_extract('src', ['rdub'], type='int', alias='rdub') }},
        {{ trx_json_extract('src', ['rdub_kw'], type='varchar', alias='rdub_kw') }},
        {{ trx_json_extract('src', ['erub'], type='varchar', alias='erub') }},
        {{ trx_json_extract('src', ['tmuc'], type='int', alias='tmuc') }},
        {{ trx_json_extract('src', ['tmuc_kw'], type='varchar', alias='tmuc_kw') }},
        {{ trx_json_extract('src', ['rduc'], type='int', alias='rduc') }},
        {{ trx_json_extract('src', ['rduc_kw'], type='varchar', alias='rduc_kw') }},
        {{ trx_json_extract('src', ['eruc'], type='varchar', alias='eruc') }},
        {{ trx_json_extract('src', ['ract'], type='varchar', alias='ract') }},
        {{ trx_json_extract('src', ['fcua_ref_compnr'], type='int', alias='fcua_ref_compnr') }},
        {{ trx_json_extract('src', ['lcur_ref_compnr'], type='int', alias='lcur_ref_compnr') }},
        {{ trx_json_extract('src', ['fcub_ref_compnr'], type='int', alias='fcub_ref_compnr') }},
        {{ trx_json_extract('src', ['fcuc_ref_compnr'], type='int', alias='fcuc_ref_compnr') }},
        {{ trx_json_extract('src', ['clcu_ref_compnr'], type='int', alias='clcu_ref_compnr') }},
        {{ trx_json_extract('src', ['ccal_ref_compnr'], type='int', alias='ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['tzid_ref_compnr'], type='int', alias='tzid_ref_compnr') }},
        {{ trx_json_extract('src', ['euro_ref_compnr'], type='int', alias='euro_ref_compnr') }},
        {{ trx_json_extract('src', ['erub_ref_compnr'], type='int', alias='erub_ref_compnr') }},
        {{ trx_json_extract('src', ['eruc_ref_compnr'], type='int', alias='eruc_ref_compnr') }},
        {{ trx_json_extract('src', ['ract_ref_compnr'], type='int', alias='ract_ref_compnr') }},
        {{ trx_json_extract('src', ['dcur_comp'], type='varchar', alias='dcur_comp') }},
        {{ trx_json_extract('src', ['taxo_rcmp'], type='int', alias='taxo_rcmp') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcemm170') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'comp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
