{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpptc353 Actual Budget by Subcontracting Control Code table, Generated 2026-04-10T19:42:27Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['ccsb'], type='varchar', alias='ccsb') }},
        {{ trx_json_extract('src', ['amoc_1'], type='float', alias='amoc_1') }},
        {{ trx_json_extract('src', ['amoc_2'], type='float', alias='amoc_2') }},
        {{ trx_json_extract('src', ['amoc_3'], type='float', alias='amoc_3') }},
        {{ trx_json_extract('src', ['amoc_4'], type='float', alias='amoc_4') }},
        {{ trx_json_extract('src', ['quan'], type='float', alias='quan') }},
        {{ trx_json_extract('src', ['cprj_ccal_ref_compnr'], type='int', alias='cprj_ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpptc353') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'ccal', 'ccsb']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
