{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN ttdpm530 IMS Parameters table, Generated 2026-04-10T19:42:40Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['sequ'], type='int', alias='sequ') }},
        {{ trx_json_extract('src', ['burl'], type='varchar', alias='burl') }},
        {{ trx_json_extract('src', ['oack'], type='varchar', alias='oack') }},
        {{ trx_json_extract('src', ['oask'], type='varchar', alias='oask') }},
        {{ trx_json_extract('src', ['loid'], type='varchar', alias='loid') }},
        {{ trx_json_extract('src', ['llid'], type='varchar', alias='llid') }},
        {{ trx_json_extract('src', ['rurl'], type='varchar', alias='rurl') }},
        {{ trx_json_extract('src', ['rack'], type='varchar', alias='rack') }},
        {{ trx_json_extract('src', ['rask'], type='varchar', alias='rask') }},
        {{ trx_json_extract('src', ['pmil'], type='int', alias='pmil') }},
        {{ trx_json_extract('src', ['pmil_kw'], type='varchar', alias='pmil_kw') }},
        {{ trx_json_extract('src', ['pmch'], type='int', alias='pmch') }},
        {{ trx_json_extract('src', ['pmch_kw'], type='varchar', alias='pmch_kw') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_ttdpm530') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'sequ']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
