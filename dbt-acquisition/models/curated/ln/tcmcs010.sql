{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs010 Countries table, Generated 2026-04-10T19:41:09Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['ccty'], type='varchar', alias='ccty') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['ictc'], type='varchar', alias='ictc') }},
        {{ trx_json_extract('src', ['tfcd'], type='varchar', alias='tfcd') }},
        {{ trx_json_extract('src', ['txcd'], type='varchar', alias='txcd') }},
        {{ trx_json_extract('src', ['fxcd'], type='varchar', alias='fxcd') }},
        {{ trx_json_extract('src', ['ssgn'], type='varchar', alias='ssgn') }},
        {{ trx_json_extract('src', ['esgn'], type='varchar', alias='esgn') }},
        {{ trx_json_extract('src', ['xsgn'], type='varchar', alias='xsgn') }},
        {{ trx_json_extract('src', ['meec'], type='int', alias='meec') }},
        {{ trx_json_extract('src', ['meec_kw'], type='varchar', alias='meec_kw') }},
        {{ trx_json_extract('src', ['geoc'], type='varchar', alias='geoc') }},
        {{ trx_json_extract('src', ['pltx'], type='int', alias='pltx') }},
        {{ trx_json_extract('src', ['pltx_kw'], type='varchar', alias='pltx_kw') }},
        {{ trx_json_extract('src', ['ptta'], type='int', alias='ptta') }},
        {{ trx_json_extract('src', ['ptta_kw'], type='varchar', alias='ptta_kw') }},
        {{ trx_json_extract('src', ['txmp'], type='int', alias='txmp') }},
        {{ trx_json_extract('src', ['txmp_kw'], type='varchar', alias='txmp_kw') }},
        {{ trx_json_extract('src', ['coaf'], type='varchar', alias='coaf') }},
        {{ trx_json_extract('src', ['afal'], type='varchar', alias='afal') }},
        {{ trx_json_extract('src', ['zmsk'], type='varchar', alias='zmsk') }},
        {{ trx_json_extract('src', ['ppcd'], type='int', alias='ppcd') }},
        {{ trx_json_extract('src', ['cgp1'], type='varchar', alias='cgp1') }},
        {{ trx_json_extract('src', ['cgp2'], type='varchar', alias='cgp2') }},
        {{ trx_json_extract('src', ['vnch'], type='int', alias='vnch') }},
        {{ trx_json_extract('src', ['vnch_kw'], type='varchar', alias='vnch_kw') }},
        {{ trx_json_extract('src', ['bnch'], type='int', alias='bnch') }},
        {{ trx_json_extract('src', ['bnch_kw'], type='varchar', alias='bnch_kw') }},
        {{ trx_json_extract('src', ['awtn'], type='int', alias='awtn') }},
        {{ trx_json_extract('src', ['awtn_kw'], type='varchar', alias='awtn_kw') }},
        {{ trx_json_extract('src', ['tzid'], type='varchar', alias='tzid') }},
        {{ trx_json_extract('src', ['clan'], type='varchar', alias='clan') }},
        {{ trx_json_extract('src', ['ict2'], type='varchar', alias='ict2') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['natl', 'en_US'], type='varchar', alias='natl__en_us') }},
        {{ trx_json_extract('src', ['ivrc'], type='varchar', alias='ivrc') }},
        {{ trx_json_extract('src', ['uarc'], type='int', alias='uarc') }},
        {{ trx_json_extract('src', ['uarc_kw'], type='varchar', alias='uarc_kw') }},
        {{ trx_json_extract('src', ['coaf_ref_compnr'], type='int', alias='coaf_ref_compnr') }},
        {{ trx_json_extract('src', ['afal_ref_compnr'], type='int', alias='afal_ref_compnr') }},
        {{ trx_json_extract('src', ['cgp1_ref_compnr'], type='int', alias='cgp1_ref_compnr') }},
        {{ trx_json_extract('src', ['cgp2_ref_compnr'], type='int', alias='cgp2_ref_compnr') }},
        {{ trx_json_extract('src', ['tzid_ref_compnr'], type='int', alias='tzid_ref_compnr') }},
        {{ trx_json_extract('src', ['clan_ref_compnr'], type='int', alias='clan_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs010') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'ccty']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
