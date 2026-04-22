{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfacp251 Invoices Related To Purchase Goods Receipt Lines table, Generated 2026-04-10T19:41:29Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['icom'], type='int', alias='icom') }},
        {{ trx_json_extract('src', ['ityp'], type='varchar', alias='ityp') }},
        {{ trx_json_extract('src', ['idoc'], type='int', alias='idoc') }},
        {{ trx_json_extract('src', ['loco'], type='int', alias='loco') }},
        {{ trx_json_extract('src', ['shpm'], type='varchar', alias='shpm') }},
        {{ trx_json_extract('src', ['otyp'], type='int', alias='otyp') }},
        {{ trx_json_extract('src', ['otyp_kw'], type='varchar', alias='otyp_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['sqnb'], type='int', alias='sqnb') }},
        {{ trx_json_extract('src', ['rcno'], type='varchar', alias='rcno') }},
        {{ trx_json_extract('src', ['rseq'], type='int', alias='rseq') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['ifbp'], type='varchar', alias='ifbp') }},
        {{ trx_json_extract('src', ['rate_1'], type='float', alias='rate_1') }},
        {{ trx_json_extract('src', ['rate_2'], type='float', alias='rate_2') }},
        {{ trx_json_extract('src', ['rate_3'], type='float', alias='rate_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['ratd'], type='timestamp_ntz', alias='ratd') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['iamt'], type='float', alias='iamt') }},
        {{ trx_json_extract('src', ['iqan'], type='float', alias='iqan') }},
        {{ trx_json_extract('src', ['iqpc'], type='float', alias='iqpc') }},
        {{ trx_json_extract('src', ['amth_1'], type='float', alias='amth_1') }},
        {{ trx_json_extract('src', ['amth_2'], type='float', alias='amth_2') }},
        {{ trx_json_extract('src', ['amth_3'], type='float', alias='amth_3') }},
        {{ trx_json_extract('src', ['finl'], type='int', alias='finl') }},
        {{ trx_json_extract('src', ['finl_kw'], type='varchar', alias='finl_kw') }},
        {{ trx_json_extract('src', ['pric'], type='int', alias='pric') }},
        {{ trx_json_extract('src', ['pric_kw'], type='varchar', alias='pric_kw') }},
        {{ trx_json_extract('src', ['data'], type='timestamp_ntz', alias='data') }},
        {{ trx_json_extract('src', ['apry'], type='int', alias='apry') }},
        {{ trx_json_extract('src', ['aprp'], type='int', alias='aprp') }},
        {{ trx_json_extract('src', ['dbmo'], type='int', alias='dbmo') }},
        {{ trx_json_extract('src', ['dbmo_kw'], type='varchar', alias='dbmo_kw') }},
        {{ trx_json_extract('src', ['buex'], type='int', alias='buex') }},
        {{ trx_json_extract('src', ['buex_kw'], type='varchar', alias='buex_kw') }},
        {{ trx_json_extract('src', ['imrf', 'en_US'], type='varchar', alias='imrf__en_us') }},
        {{ trx_json_extract('src', ['pdif'], type='float', alias='pdif') }},
        {{ trx_json_extract('src', ['pdfd'], type='int', alias='pdfd') }},
        {{ trx_json_extract('src', ['pdfd_kw'], type='varchar', alias='pdfd_kw') }},
        {{ trx_json_extract('src', ['shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr'], type='int', alias='shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr') }},
        {{ trx_json_extract('src', ['ifbp_ref_compnr'], type='int', alias='ifbp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfacp251') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'icom', 'ityp', 'idoc', 'loco', 'shpm', 'otyp', 'orno', 'pono', 'sqnb', 'rcno', 'rseq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
