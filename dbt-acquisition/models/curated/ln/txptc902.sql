{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN txptc902 Project Risks table, Generated 2026-04-10T19:42:42Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['prsk'], type='varchar', alias='prsk') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['ersk'], type='varchar', alias='ersk') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['creq'], type='int', alias='creq') }},
        {{ trx_json_extract('src', ['creq_kw'], type='varchar', alias='creq_kw') }},
        {{ trx_json_extract('src', ['vreq'], type='int', alias='vreq') }},
        {{ trx_json_extract('src', ['vreq_kw'], type='varchar', alias='vreq_kw') }},
        {{ trx_json_extract('src', ['rcat'], type='varchar', alias='rcat') }},
        {{ trx_json_extract('src', ['tdat'], type='date', alias='tdat') }},
        {{ trx_json_extract('src', ['cdat'], type='timestamp_ntz', alias='cdat') }},
        {{ trx_json_extract('src', ['crno'], type='varchar', alias='crno') }},
        {{ trx_json_extract('src', ['crpo'], type='int', alias='crpo') }},
        {{ trx_json_extract('src', ['vrno'], type='varchar', alias='vrno') }},
        {{ trx_json_extract('src', ['vrpo'], type='int', alias='vrpo') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_txptc902') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'prsk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
