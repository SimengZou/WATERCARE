{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfacp245 Goods Receipts table, Generated 2026-04-10T19:41:29Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['shpm'], type='varchar', alias='shpm') }},
        {{ trx_json_extract('src', ['otyp'], type='int', alias='otyp') }},
        {{ trx_json_extract('src', ['otyp_kw'], type='varchar', alias='otyp_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['sqnb'], type='int', alias='sqnb') }},
        {{ trx_json_extract('src', ['loco'], type='int', alias='loco') }},
        {{ trx_json_extract('src', ['rcno'], type='varchar', alias='rcno') }},
        {{ trx_json_extract('src', ['rseq'], type='int', alias='rseq') }},
        {{ trx_json_extract('src', ['bonm'], type='varchar', alias='bonm') }},
        {{ trx_json_extract('src', ['boid'], type='varchar', alias='boid') }},
        {{ trx_json_extract('src', ['borf'], type='varchar', alias='borf') }},
        {{ trx_json_extract('src', ['rgui'], type='varchar', alias='rgui') }},
        {{ trx_json_extract('src', ['fcpo'], type='int', alias='fcpo') }},
        {{ trx_json_extract('src', ['otbp'], type='varchar', alias='otbp') }},
        {{ trx_json_extract('src', ['rqty'], type='float', alias='rqty') }},
        {{ trx_json_extract('src', ['rqpc'], type='float', alias='rqpc') }},
        {{ trx_json_extract('src', ['quan'], type='float', alias='quan') }},
        {{ trx_json_extract('src', ['aqpc'], type='float', alias='aqpc') }},
        {{ trx_json_extract('src', ['trqn'], type='float', alias='trqn') }},
        {{ trx_json_extract('src', ['rjpc'], type='float', alias='rjpc') }},
        {{ trx_json_extract('src', ['toma'], type='int', alias='toma') }},
        {{ trx_json_extract('src', ['toma_kw'], type='varchar', alias='toma_kw') }},
        {{ trx_json_extract('src', ['sfbl'], type='int', alias='sfbl') }},
        {{ trx_json_extract('src', ['sfbl_kw'], type='varchar', alias='sfbl_kw') }},
        {{ trx_json_extract('src', ['rdat'], type='timestamp_ntz', alias='rdat') }},
        {{ trx_json_extract('src', ['mpnr'], type='varchar', alias='mpnr') }},
        {{ trx_json_extract('src', ['cmnf'], type='varchar', alias='cmnf') }},
        {{ trx_json_extract('src', ['sdat'], type='timestamp_ntz', alias='sdat') }},
        {{ trx_json_extract('src', ['sbdt'], type='int', alias='sbdt') }},
        {{ trx_json_extract('src', ['sbdt_kw'], type='varchar', alias='sbdt_kw') }},
        {{ trx_json_extract('src', ['imrf', 'en_US'], type='varchar', alias='imrf__en_us') }},
        {{ trx_json_extract('src', ['mtch'], type='int', alias='mtch') }},
        {{ trx_json_extract('src', ['mtch_kw'], type='varchar', alias='mtch_kw') }},
        {{ trx_json_extract('src', ['maqu'], type='float', alias='maqu') }},
        {{ trx_json_extract('src', ['mqpc'], type='float', alias='mqpc') }},
        {{ trx_json_extract('src', ['mqpu'], type='float', alias='mqpu') }},
        {{ trx_json_extract('src', ['maam'], type='float', alias='maam') }},
        {{ trx_json_extract('src', ['lmat'], type='int', alias='lmat') }},
        {{ trx_json_extract('src', ['lmat_kw'], type='varchar', alias='lmat_kw') }},
        {{ trx_json_extract('src', ['omti'], type='int', alias='omti') }},
        {{ trx_json_extract('src', ['omti_kw'], type='varchar', alias='omti_kw') }},
        {{ trx_json_extract('src', ['uppr'], type='int', alias='uppr') }},
        {{ trx_json_extract('src', ['uppr_kw'], type='varchar', alias='uppr_kw') }},
        {{ trx_json_extract('src', ['cval'], type='float', alias='cval') }},
        {{ trx_json_extract('src', ['cvlc'], type='varchar', alias='cvlc') }},
        {{ trx_json_extract('src', ['cdf_txta'], type='int', alias='cdf_txta') }},
        {{ trx_json_extract('src', ['loco_otyp_orno_pono_sqnb_ref_compnr'], type='int', alias='loco_otyp_orno_pono_sqnb_ref_compnr') }},
        {{ trx_json_extract('src', ['cvlc_ref_compnr'], type='int', alias='cvlc_ref_compnr') }},
        {{ trx_json_extract('src', ['cdf_txta_ref_compnr'], type='int', alias='cdf_txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfacp245') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'shpm', 'otyp', 'orno', 'pono', 'sqnb', 'loco', 'rcno', 'rseq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
