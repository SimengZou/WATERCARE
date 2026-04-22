{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tirou101 Routing Codes by Item table, Generated 2026-04-10T19:41:49Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['mitm'], type='varchar', alias='mitm') }},
        {{ trx_json_extract('src', ['opro'], type='varchar', alias='opro') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['stor'], type='int', alias='stor') }},
        {{ trx_json_extract('src', ['stor_kw'], type='varchar', alias='stor_kw') }},
        {{ trx_json_extract('src', ['strc'], type='varchar', alias='strc') }},
        {{ trx_json_extract('src', ['woar'], type='varchar', alias='woar') }},
        {{ trx_json_extract('src', ['ruoq'], type='float', alias='ruoq') }},
        {{ trx_json_extract('src', ['runi'], type='float', alias='runi') }},
        {{ trx_json_extract('src', ['rcal'], type='float', alias='rcal') }},
        {{ trx_json_extract('src', ['bwce'], type='varchar', alias='bwce') }},
        {{ trx_json_extract('src', ['ract'], type='float', alias='ract') }},
        {{ trx_json_extract('src', ['bwca'], type='varchar', alias='bwca') }},
        {{ trx_json_extract('src', ['lcdr'], type='timestamp_ntz', alias='lcdr') }},
        {{ trx_json_extract('src', ['mrau'], type='int', alias='mrau') }},
        {{ trx_json_extract('src', ['mrau_kw'], type='varchar', alias='mrau_kw') }},
        {{ trx_json_extract('src', ['lmdt'], type='timestamp_ntz', alias='lmdt') }},
        {{ trx_json_extract('src', ['unef'], type='int', alias='unef') }},
        {{ trx_json_extract('src', ['unef_kw'], type='varchar', alias='unef_kw') }},
        {{ trx_json_extract('src', ['stcf'], type='int', alias='stcf') }},
        {{ trx_json_extract('src', ['stcf_kw'], type='varchar', alias='stcf_kw') }},
        {{ trx_json_extract('src', ['wkbt'], type='int', alias='wkbt') }},
        {{ trx_json_extract('src', ['wkbt_kw'], type='varchar', alias='wkbt_kw') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['mitm_ref_compnr'], type='int', alias='mitm_ref_compnr') }},
        {{ trx_json_extract('src', ['woar_ref_compnr'], type='int', alias='woar_ref_compnr') }},
        {{ trx_json_extract('src', ['bwce_ref_compnr'], type='int', alias='bwce_ref_compnr') }},
        {{ trx_json_extract('src', ['bwca_ref_compnr'], type='int', alias='bwca_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tirou101') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'mitm', 'opro']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
