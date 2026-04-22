{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tdpur411 Purchase Order Line Item Data table, Generated 2026-04-10T19:41:22Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['sqnb'], type='int', alias='sqnb') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['atse'], type='varchar', alias='atse') }},
        {{ trx_json_extract('src', ['atsg'], type='varchar', alias='atsg') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['citg'], type='varchar', alias='citg') }},
        {{ trx_json_extract('src', ['cpcl'], type='varchar', alias='cpcl') }},
        {{ trx_json_extract('src', ['cpln'], type='varchar', alias='cpln') }},
        {{ trx_json_extract('src', ['prtp'], type='varchar', alias='prtp') }},
        {{ trx_json_extract('src', ['cpgp'], type='varchar', alias='cpgp') }},
        {{ trx_json_extract('src', ['csgp'], type='varchar', alias='csgp') }},
        {{ trx_json_extract('src', ['iwgt'], type='float', alias='iwgt') }},
        {{ trx_json_extract('src', ['iwun'], type='varchar', alias='iwun') }},
        {{ trx_json_extract('src', ['pgmd'], type='int', alias='pgmd') }},
        {{ trx_json_extract('src', ['pgmd_kw'], type='varchar', alias='pgmd_kw') }},
        {{ trx_json_extract('src', ['kitm'], type='int', alias='kitm') }},
        {{ trx_json_extract('src', ['kitm_kw'], type='varchar', alias='kitm_kw') }},
        {{ trx_json_extract('src', ['orno_pono_sqnb_ref_compnr'], type='int', alias='orno_pono_sqnb_ref_compnr') }},
        {{ trx_json_extract('src', ['orno_ref_compnr'], type='int', alias='orno_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['atse_ref_compnr'], type='int', alias='atse_ref_compnr') }},
        {{ trx_json_extract('src', ['atsg_ref_compnr'], type='int', alias='atsg_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['citg_ref_compnr'], type='int', alias='citg_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcl_ref_compnr'], type='int', alias='cpcl_ref_compnr') }},
        {{ trx_json_extract('src', ['cpln_ref_compnr'], type='int', alias='cpln_ref_compnr') }},
        {{ trx_json_extract('src', ['prtp_ref_compnr'], type='int', alias='prtp_ref_compnr') }},
        {{ trx_json_extract('src', ['cpgp_ref_compnr'], type='int', alias='cpgp_ref_compnr') }},
        {{ trx_json_extract('src', ['csgp_ref_compnr'], type='int', alias='csgp_ref_compnr') }},
        {{ trx_json_extract('src', ['iwun_ref_compnr'], type='int', alias='iwun_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tdpur411') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'orno', 'pono', 'sqnb']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
