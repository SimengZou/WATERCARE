{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tccom101 Business Partner Defaults table, Generated 2026-04-10T19:41:01Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['sern'], type='int', alias='sern') }},
        {{ trx_json_extract('src', ['clan'], type='varchar', alias='clan') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['ctyp'], type='varchar', alias='ctyp') }},
        {{ trx_json_extract('src', ['cfir'], type='int', alias='cfir') }},
        {{ trx_json_extract('src', ['cfir_kw'], type='varchar', alias='cfir_kw') }},
        {{ trx_json_extract('src', ['cinm'], type='varchar', alias='cinm') }},
        {{ trx_json_extract('src', ['crat'], type='varchar', alias='crat') }},
        {{ trx_json_extract('src', ['cpay'], type='varchar', alias='cpay') }},
        {{ trx_json_extract('src', ['pcur'], type='varchar', alias='pcur') }},
        {{ trx_json_extract('src', ['cfcg'], type='varchar', alias='cfcg') }},
        {{ trx_json_extract('src', ['scur'], type='varchar', alias='scur') }},
        {{ trx_json_extract('src', ['styp'], type='varchar', alias='styp') }},
        {{ trx_json_extract('src', ['sfir'], type='int', alias='sfir') }},
        {{ trx_json_extract('src', ['sfir_kw'], type='varchar', alias='sfir_kw') }},
        {{ trx_json_extract('src', ['spay'], type='varchar', alias='spay') }},
        {{ trx_json_extract('src', ['cfsg'], type='varchar', alias='cfsg') }},
        {{ trx_json_extract('src', ['lmdt'], type='timestamp_ntz', alias='lmdt') }},
        {{ trx_json_extract('src', ['btbv'], type='int', alias='btbv') }},
        {{ trx_json_extract('src', ['btbv_kw'], type='varchar', alias='btbv_kw') }},
        {{ trx_json_extract('src', ['bprl'], type='int', alias='bprl') }},
        {{ trx_json_extract('src', ['bprl_kw'], type='varchar', alias='bprl_kw') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['clan_ref_compnr'], type='int', alias='clan_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['ctyp_ref_compnr'], type='int', alias='ctyp_ref_compnr') }},
        {{ trx_json_extract('src', ['cinm_ref_compnr'], type='int', alias='cinm_ref_compnr') }},
        {{ trx_json_extract('src', ['crat_ref_compnr'], type='int', alias='crat_ref_compnr') }},
        {{ trx_json_extract('src', ['cpay_ref_compnr'], type='int', alias='cpay_ref_compnr') }},
        {{ trx_json_extract('src', ['pcur_ref_compnr'], type='int', alias='pcur_ref_compnr') }},
        {{ trx_json_extract('src', ['scur_ref_compnr'], type='int', alias='scur_ref_compnr') }},
        {{ trx_json_extract('src', ['styp_ref_compnr'], type='int', alias='styp_ref_compnr') }},
        {{ trx_json_extract('src', ['spay_ref_compnr'], type='int', alias='spay_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tccom101') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'sern']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
