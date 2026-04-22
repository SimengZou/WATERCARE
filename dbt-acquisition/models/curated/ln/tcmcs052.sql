{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs052 Projects table, Generated 2026-04-10T19:41:12Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['dscb', 'en_US'], type='varchar', alias='dscb__en_us') }},
        {{ trx_json_extract('src', ['dscc', 'en_US'], type='varchar', alias='dscc__en_us') }},
        {{ trx_json_extract('src', ['dscd', 'en_US'], type='varchar', alias='dscd__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['seab', 'en_US'], type='varchar', alias='seab__en_us') }},
        {{ trx_json_extract('src', ['plnk'], type='int', alias='plnk') }},
        {{ trx_json_extract('src', ['plnk_kw'], type='varchar', alias='plnk_kw') }},
        {{ trx_json_extract('src', ['isrv'], type='int', alias='isrv') }},
        {{ trx_json_extract('src', ['isrv_kw'], type='varchar', alias='isrv_kw') }},
        {{ trx_json_extract('src', ['pctr'], type='varchar', alias='pctr') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs052') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
