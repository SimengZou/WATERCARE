{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN qmptc112 Sample Statistical Measures table, Generated 2022-06-15T01:21:06Z from package combination ce01055',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['iorn'], type='varchar', alias='iorn') }},
        {{ trx_json_extract('src', ['saml'], type='int', alias='saml') }},
        {{ trx_json_extract('src', ['ipon'], type='int', alias='ipon') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['aspt'], type='varchar', alias='aspt') }},
        {{ trx_json_extract('src', ['char'], type='varchar', alias='char') }},
        {{ trx_json_extract('src', ['chun'], type='varchar', alias='chun') }},
        {{ trx_json_extract('src', ['trun'], type='varchar', alias='trun') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['osta'], type='int', alias='osta') }},
        {{ trx_json_extract('src', ['osta_kw'], type='varchar', alias='osta_kw') }},
        {{ trx_json_extract('src', ['acre'], type='int', alias='acre') }},
        {{ trx_json_extract('src', ['acre_kw'], type='varchar', alias='acre_kw') }},
        {{ trx_json_extract('src', ['cdat'], type='timestamp_ntz', alias='cdat') }},
        {{ trx_json_extract('src', ['orgn'], type='int', alias='orgn') }},
        {{ trx_json_extract('src', ['orgn_kw'], type='varchar', alias='orgn_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['opno'], type='int', alias='opno') }},
        {{ trx_json_extract('src', ['wstt'], type='varchar', alias='wstt') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['apno'], type='varchar', alias='apno') }},
        {{ trx_json_extract('src', ['apsq'], type='int', alias='apsq') }},
        {{ trx_json_extract('src', ['huid'], type='varchar', alias='huid') }},
        {{ trx_json_extract('src', ['mean'], type='float', alias='mean') }},
        {{ trx_json_extract('src', ['mrav'], type='float', alias='mrav') }},
        {{ trx_json_extract('src', ['rang'], type='float', alias='rang') }},
        {{ trx_json_extract('src', ['stdv'], type='float', alias='stdv') }},
        {{ trx_json_extract('src', ['nosp'], type='int', alias='nosp') }},
        {{ trx_json_extract('src', ['mivl'], type='float', alias='mivl') }},
        {{ trx_json_extract('src', ['mavl'], type='float', alias='mavl') }},
        {{ trx_json_extract('src', ['clwi'], type='float', alias='clwi') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['bpid_ref_compnr'], type='int', alias='bpid_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_qmptc112') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'iorn', 'saml', 'ipon']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
