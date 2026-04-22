{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam840 Disposal Transaction table, Generated 2026-04-10T19:41:36Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['tkey'], type='int', alias='tkey') }},
        {{ trx_json_extract('src', ['adrt'], type='int', alias='adrt') }},
        {{ trx_json_extract('src', ['adrt_kw'], type='varchar', alias='adrt_kw') }},
        {{ trx_json_extract('src', ['cost_1'], type='float', alias='cost_1') }},
        {{ trx_json_extract('src', ['cost_2'], type='float', alias='cost_2') }},
        {{ trx_json_extract('src', ['cost_3'], type='float', alias='cost_3') }},
        {{ trx_json_extract('src', ['curr'], type='varchar', alias='curr') }},
        {{ trx_json_extract('src', ['dqty'], type='float', alias='dqty') }},
        {{ trx_json_extract('src', ['jobn'], type='int', alias='jobn') }},
        {{ trx_json_extract('src', ['life'], type='int', alias='life') }},
        {{ trx_json_extract('src', ['ltdd_1'], type='float', alias='ltdd_1') }},
        {{ trx_json_extract('src', ['ltdd_2'], type='float', alias='ltdd_2') }},
        {{ trx_json_extract('src', ['ltdd_3'], type='float', alias='ltdd_3') }},
        {{ trx_json_extract('src', ['proc_1'], type='float', alias='proc_1') }},
        {{ trx_json_extract('src', ['proc_2'], type='float', alias='proc_2') }},
        {{ trx_json_extract('src', ['proc_3'], type='float', alias='proc_3') }},
        {{ trx_json_extract('src', ['prod'], type='int', alias='prod') }},
        {{ trx_json_extract('src', ['prot'], type='float', alias='prot') }},
        {{ trx_json_extract('src', ['ratd'], type='timestamp_ntz', alias='ratd') }},
        {{ trx_json_extract('src', ['rate_1'], type='float', alias='rate_1') }},
        {{ trx_json_extract('src', ['rate_2'], type='float', alias='rate_2') }},
        {{ trx_json_extract('src', ['rate_3'], type='float', alias='rate_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['reas'], type='varchar', alias='reas') }},
        {{ trx_json_extract('src', ['refn'], type='varchar', alias='refn') }},
        {{ trx_json_extract('src', ['salv_1'], type='float', alias='salv_1') }},
        {{ trx_json_extract('src', ['salv_2'], type='float', alias='salv_2') }},
        {{ trx_json_extract('src', ['salv_3'], type='float', alias='salv_3') }},
        {{ trx_json_extract('src', ['tagn', 'en_US'], type='varchar', alias='tagn__en_us') }},
        {{ trx_json_extract('src', ['type'], type='int', alias='type') }},
        {{ trx_json_extract('src', ['type_kw'], type='varchar', alias='type_kw') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['rcst_1'], type='float', alias='rcst_1') }},
        {{ trx_json_extract('src', ['rcst_2'], type='float', alias='rcst_2') }},
        {{ trx_json_extract('src', ['rcst_3'], type='float', alias='rcst_3') }},
        {{ trx_json_extract('src', ['ltdr_1'], type='float', alias='ltdr_1') }},
        {{ trx_json_extract('src', ['ltdr_2'], type='float', alias='ltdr_2') }},
        {{ trx_json_extract('src', ['ltdr_3'], type='float', alias='ltdr_3') }},
        {{ trx_json_extract('src', ['tkey_ref_compnr'], type='int', alias='tkey_ref_compnr') }},
        {{ trx_json_extract('src', ['reas_ref_compnr'], type='int', alias='reas_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam840') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'tkey']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
