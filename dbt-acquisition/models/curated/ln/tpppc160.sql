{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpppc160 Activity Physical Progress table, Generated 2026-04-10T19:42:06Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['date'], type='timestamp_ntz', alias='date') }},
        {{ trx_json_extract('src', ['prst'], type='float', alias='prst') }},
        {{ trx_json_extract('src', ['quan'], type='float', alias='quan') }},
        {{ trx_json_extract('src', ['prco'], type='int', alias='prco') }},
        {{ trx_json_extract('src', ['prco_kw'], type='varchar', alias='prco_kw') }},
        {{ trx_json_extract('src', ['appr'], type='int', alias='appr') }},
        {{ trx_json_extract('src', ['appr_kw'], type='varchar', alias='appr_kw') }},
        {{ trx_json_extract('src', ['txtn'], type='int', alias='txtn') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['txtn_ref_compnr'], type='int', alias='txtn_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpppc160') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'cpla', 'cact', 'date']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
