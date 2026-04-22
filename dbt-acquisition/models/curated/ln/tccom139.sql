{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tccom139 Cities by Country table, Generated 2026-04-10T19:41:04Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['ccty'], type='varchar', alias='ccty') }},
        {{ trx_json_extract('src', ['cste'], type='varchar', alias='cste') }},
        {{ trx_json_extract('src', ['city'], type='varchar', alias='city') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['idnr'], type='int', alias='idnr') }},
        {{ trx_json_extract('src', ['ccty_ref_compnr'], type='int', alias='ccty_ref_compnr') }},
        {{ trx_json_extract('src', ['ccty_cste_ref_compnr'], type='int', alias='ccty_cste_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tccom139') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'ccty', 'cste', 'city']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
