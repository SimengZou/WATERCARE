{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpctm060 Contract Link to Project Structure table, Generated 2026-04-10T19:41:50Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cono'], type='varchar', alias='cono') }},
        {{ trx_json_extract('src', ['sern'], type='int', alias='sern') }},
        {{ trx_json_extract('src', ['cnln'], type='varchar', alias='cnln') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['cstl'], type='varchar', alias='cstl') }},
        {{ trx_json_extract('src', ['ccho'], type='varchar', alias='ccho') }},
        {{ trx_json_extract('src', ['chln'], type='varchar', alias='chln') }},
        {{ trx_json_extract('src', ['cono_cnln_ref_compnr'], type='int', alias='cono_cnln_ref_compnr') }},
        {{ trx_json_extract('src', ['cono_ref_compnr'], type='int', alias='cono_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cspa_ref_compnr'], type='int', alias='cprj_cspa_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cstl_ref_compnr'], type='int', alias='cprj_cstl_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpctm060') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cono', 'sern']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
