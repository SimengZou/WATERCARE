{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam120 Asset Distribution table, Generated 2026-04-10T19:41:32Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['anbr'], type='varchar', alias='anbr') }},
        {{ trx_json_extract('src', ['aext'], type='varchar', alias='aext') }},
        {{ trx_json_extract('src', ['dkey'], type='int', alias='dkey') }},
        {{ trx_json_extract('src', ['comp'], type='int', alias='comp') }},
        {{ trx_json_extract('src', ['dqty'], type='int', alias='dqty') }},
        {{ trx_json_extract('src', ['lkey'], type='int', alias='lkey') }},
        {{ trx_json_extract('src', ['memo', 'en_US'], type='varchar', alias='memo__en_us') }},
        {{ trx_json_extract('src', ['srce'], type='int', alias='srce') }},
        {{ trx_json_extract('src', ['srce_kw'], type='varchar', alias='srce_kw') }},
        {{ trx_json_extract('src', ['trsc'], type='varchar', alias='trsc') }},
        {{ trx_json_extract('src', ['anbr_aext_ref_compnr'], type='int', alias='anbr_aext_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam120') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'anbr', 'aext', 'dkey']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
