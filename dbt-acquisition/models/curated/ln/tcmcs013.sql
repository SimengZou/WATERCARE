{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs013 Payment Terms table, Generated 2026-04-10T19:41:10Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cpay'], type='varchar', alias='cpay') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['pper'], type='int', alias='pper') }},
        {{ trx_json_extract('src', ['pash'], type='varchar', alias='pash') }},
        {{ trx_json_extract('src', ['disa'], type='int', alias='disa') }},
        {{ trx_json_extract('src', ['disb'], type='int', alias='disb') }},
        {{ trx_json_extract('src', ['disc'], type='int', alias='disc') }},
        {{ trx_json_extract('src', ['prca'], type='float', alias='prca') }},
        {{ trx_json_extract('src', ['prcb'], type='float', alias='prcb') }},
        {{ trx_json_extract('src', ['prcc'], type='float', alias='prcc') }},
        {{ trx_json_extract('src', ['atie'], type='int', alias='atie') }},
        {{ trx_json_extract('src', ['atie_kw'], type='varchar', alias='atie_kw') }},
        {{ trx_json_extract('src', ['pdin'], type='int', alias='pdin') }},
        {{ trx_json_extract('src', ['pdin_kw'], type='varchar', alias='pdin_kw') }},
        {{ trx_json_extract('src', ['cdde'], type='int', alias='cdde') }},
        {{ trx_json_extract('src', ['cdde_kw'], type='varchar', alias='cdde_kw') }},
        {{ trx_json_extract('src', ['cdis'], type='int', alias='cdis') }},
        {{ trx_json_extract('src', ['cdis_kw'], type='varchar', alias='cdis_kw') }},
        {{ trx_json_extract('src', ['fdue'], type='int', alias='fdue') }},
        {{ trx_json_extract('src', ['fdis'], type='int', alias='fdis') }},
        {{ trx_json_extract('src', ['told'], type='float', alias='told') }},
        {{ trx_json_extract('src', ['tola'], type='float', alias='tola') }},
        {{ trx_json_extract('src', ['tolp'], type='int', alias='tolp') }},
        {{ trx_json_extract('src', ['tlsd'], type='int', alias='tlsd') }},
        {{ trx_json_extract('src', ['day1'], type='int', alias='day1') }},
        {{ trx_json_extract('src', ['day2'], type='int', alias='day2') }},
        {{ trx_json_extract('src', ['day3'], type='int', alias='day3') }},
        {{ trx_json_extract('src', ['ptyp'], type='int', alias='ptyp') }},
        {{ trx_json_extract('src', ['ptyp_kw'], type='varchar', alias='ptyp_kw') }},
        {{ trx_json_extract('src', ['prio'], type='int', alias='prio') }},
        {{ trx_json_extract('src', ['prio_kw'], type='varchar', alias='prio_kw') }},
        {{ trx_json_extract('src', ['pdyn'], type='int', alias='pdyn') }},
        {{ trx_json_extract('src', ['pdyn_kw'], type='varchar', alias='pdyn_kw') }},
        {{ trx_json_extract('src', ['pdis'], type='int', alias='pdis') }},
        {{ trx_json_extract('src', ['pdis_kw'], type='varchar', alias='pdis_kw') }},
        {{ trx_json_extract('src', ['dtbs'], type='int', alias='dtbs') }},
        {{ trx_json_extract('src', ['dtbs_kw'], type='varchar', alias='dtbs_kw') }},
        {{ trx_json_extract('src', ['vtpm'], type='varchar', alias='vtpm') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['pash_ref_compnr'], type='int', alias='pash_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs013') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cpay']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
