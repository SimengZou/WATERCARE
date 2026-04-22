{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs064 Credit Ratings table, Generated 2026-04-10T19:41:13Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['crat'], type='varchar', alias='crat') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['phs1'], type='int', alias='phs1') }},
        {{ trx_json_extract('src', ['phs1_kw'], type='varchar', alias='phs1_kw') }},
        {{ trx_json_extract('src', ['phs2'], type='int', alias='phs2') }},
        {{ trx_json_extract('src', ['phs2_kw'], type='varchar', alias='phs2_kw') }},
        {{ trx_json_extract('src', ['phs3'], type='int', alias='phs3') }},
        {{ trx_json_extract('src', ['phs3_kw'], type='varchar', alias='phs3_kw') }},
        {{ trx_json_extract('src', ['phs4'], type='int', alias='phs4') }},
        {{ trx_json_extract('src', ['phs4_kw'], type='varchar', alias='phs4_kw') }},
        {{ trx_json_extract('src', ['phs5'], type='int', alias='phs5') }},
        {{ trx_json_extract('src', ['phs5_kw'], type='varchar', alias='phs5_kw') }},
        {{ trx_json_extract('src', ['phs6'], type='int', alias='phs6') }},
        {{ trx_json_extract('src', ['phs6_kw'], type='varchar', alias='phs6_kw') }},
        {{ trx_json_extract('src', ['phs7'], type='int', alias='phs7') }},
        {{ trx_json_extract('src', ['phs7_kw'], type='varchar', alias='phs7_kw') }},
        {{ trx_json_extract('src', ['pss1'], type='int', alias='pss1') }},
        {{ trx_json_extract('src', ['pss1_kw'], type='varchar', alias='pss1_kw') }},
        {{ trx_json_extract('src', ['pss2'], type='int', alias='pss2') }},
        {{ trx_json_extract('src', ['pss2_kw'], type='varchar', alias='pss2_kw') }},
        {{ trx_json_extract('src', ['pss3'], type='int', alias='pss3') }},
        {{ trx_json_extract('src', ['pss3_kw'], type='varchar', alias='pss3_kw') }},
        {{ trx_json_extract('src', ['bldf'], type='varchar', alias='bldf') }},
        {{ trx_json_extract('src', ['actn'], type='int', alias='actn') }},
        {{ trx_json_extract('src', ['actn_kw'], type='varchar', alias='actn_kw') }},
        {{ trx_json_extract('src', ['revp'], type='int', alias='revp') }},
        {{ trx_json_extract('src', ['ioso'], type='int', alias='ioso') }},
        {{ trx_json_extract('src', ['ioso_kw'], type='varchar', alias='ioso_kw') }},
        {{ trx_json_extract('src', ['ddcc'], type='int', alias='ddcc') }},
        {{ trx_json_extract('src', ['ddcc_kw'], type='varchar', alias='ddcc_kw') }},
        {{ trx_json_extract('src', ['bldf_ref_compnr'], type='int', alias='bldf_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs064') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'crat']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
