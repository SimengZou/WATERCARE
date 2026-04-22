{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppss210 Activity Relationships table, Generated 2026-04-10T19:42:19Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['ffno'], type='varchar', alias='ffno') }},
        {{ trx_json_extract('src', ['pact'], type='varchar', alias='pact') }},
        {{ trx_json_extract('src', ['pcot'], type='int', alias='pcot') }},
        {{ trx_json_extract('src', ['pcot_kw'], type='varchar', alias='pcot_kw') }},
        {{ trx_json_extract('src', ['psen'], type='int', alias='psen') }},
        {{ trx_json_extract('src', ['sact'], type='varchar', alias='sact') }},
        {{ trx_json_extract('src', ['scot'], type='int', alias='scot') }},
        {{ trx_json_extract('src', ['scot_kw'], type='varchar', alias='scot_kw') }},
        {{ trx_json_extract('src', ['ssen'], type='int', alias='ssen') }},
        {{ trx_json_extract('src', ['rltp'], type='int', alias='rltp') }},
        {{ trx_json_extract('src', ['rltp_kw'], type='varchar', alias='rltp_kw') }},
        {{ trx_json_extract('src', ['lead'], type='float', alias='lead') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['llpc'], type='int', alias='llpc') }},
        {{ trx_json_extract('src', ['rest'], type='int', alias='rest') }},
        {{ trx_json_extract('src', ['rest_kw'], type='varchar', alias='rest_kw') }},
        {{ trx_json_extract('src', ['cprj_cpla_ref_compnr'], type='int', alias='cprj_cpla_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_pact_ref_compnr'], type='int', alias='cprj_cpla_pact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_sact_ref_compnr'], type='int', alias='cprj_cpla_sact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['cuni_ref_compnr'], type='int', alias='cuni_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppss210') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'cpla', 'ffno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
