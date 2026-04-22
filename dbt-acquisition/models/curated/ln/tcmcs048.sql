{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs048 Cost Components table, Generated 2026-04-10T19:41:12Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cpcp'], type='varchar', alias='cpcp') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['cpus'], type='int', alias='cpus') }},
        {{ trx_json_extract('src', ['cpus_kw'], type='varchar', alias='cpus_kw') }},
        {{ trx_json_extract('src', ['iinv'], type='int', alias='iinv') }},
        {{ trx_json_extract('src', ['iinv_kw'], type='varchar', alias='iinv_kw') }},
        {{ trx_json_extract('src', ['fxvc'], type='int', alias='fxvc') }},
        {{ trx_json_extract('src', ['fxvc_kw'], type='varchar', alias='fxvc_kw') }},
        {{ trx_json_extract('src', ['dinc'], type='int', alias='dinc') }},
        {{ trx_json_extract('src', ['dinc_kw'], type='varchar', alias='dinc_kw') }},
        {{ trx_json_extract('src', ['cref'], type='int', alias='cref') }},
        {{ trx_json_extract('src', ['cref_kw'], type='varchar', alias='cref_kw') }},
        {{ trx_json_extract('src', ['tpoc'], type='int', alias='tpoc') }},
        {{ trx_json_extract('src', ['tpoc_kw'], type='varchar', alias='tpoc_kw') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs048') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cpcp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
