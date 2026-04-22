{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN ttdpm510 Data Set Companies table, Generated 2026-04-10T19:42:40Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['pacc'], type='varchar', alias='pacc') }},
        {{ trx_json_extract('src', ['dset'], type='varchar', alias='dset') }},
        {{ trx_json_extract('src', ['comp'], type='int', alias='comp') }},
        {{ trx_json_extract('src', ['pubd'], type='int', alias='pubd') }},
        {{ trx_json_extract('src', ['pubd_kw'], type='varchar', alias='pubd_kw') }},
        {{ trx_json_extract('src', ['pacc_dset_ref_compnr'], type='int', alias='pacc_dset_ref_compnr') }},
        {{ trx_json_extract('src', ['pacc_ref_compnr'], type='int', alias='pacc_ref_compnr') }},
        {{ trx_json_extract('src', ['dset_ref_compnr'], type='int', alias='dset_ref_compnr') }},
        {{ trx_json_extract('src', ['comp_ref_compnr'], type='int', alias='comp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_ttdpm510') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'pacc', 'dset', 'comp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
