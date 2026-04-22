{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpppc205 Cost Entry Defaults table, Generated 2026-04-10T19:42:07Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['cstl'], type='varchar', alias='cstl') }},
        {{ trx_json_extract('src', ['rgdt'], type='timestamp_ntz', alias='rgdt') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['peri'], type='int', alias='peri') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['hyea'], type='int', alias='hyea') }},
        {{ trx_json_extract('src', ['hper'], type='int', alias='hper') }},
        {{ trx_json_extract('src', ['otbp'], type='varchar', alias='otbp') }},
        {{ trx_json_extract('src', ['entu'], type='varchar', alias='entu') }},
        {{ trx_json_extract('src', ['frdt'], type='timestamp_ntz', alias='frdt') }},
        {{ trx_json_extract('src', ['cprj_cspa_ref_compnr'], type='int', alias='cprj_cspa_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cstl_ref_compnr'], type='int', alias='cprj_cstl_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['otbp_ref_compnr'], type='int', alias='otbp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpppc205') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'user']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
