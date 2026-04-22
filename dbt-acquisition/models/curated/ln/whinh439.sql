{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinh439 Deliveries table, Generated 2026-04-10T19:42:50Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['oorg'], type='int', alias='oorg') }},
        {{ trx_json_extract('src', ['oorg_kw'], type='varchar', alias='oorg_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['sqnb'], type='int', alias='sqnb') }},
        {{ trx_json_extract('src', ['shpm'], type='varchar', alias='shpm') }},
        {{ trx_json_extract('src', ['shln'], type='int', alias='shln') }},
        {{ trx_json_extract('src', ['fshp'], type='int', alias='fshp') }},
        {{ trx_json_extract('src', ['fshp_kw'], type='varchar', alias='fshp_kw') }},
        {{ trx_json_extract('src', ['cdat'], type='timestamp_ntz', alias='cdat') }},
        {{ trx_json_extract('src', ['ldat'], type='timestamp_ntz', alias='ldat') }},
        {{ trx_json_extract('src', ['mess', 'en_US'], type='varchar', alias='mess__en_us') }},
        {{ trx_json_extract('src', ['shpm_shln_ref_compnr'], type='int', alias='shpm_shln_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinh439') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'oorg', 'orno', 'pono', 'seqn', 'sqnb', 'shpm', 'shln']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
