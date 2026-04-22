{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam810 Acquisition Transaction table, Generated 2026-04-10T19:41:35Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['tkey'], type='int', alias='tkey') }},
        {{ trx_json_extract('src', ['acmc_1'], type='float', alias='acmc_1') }},
        {{ trx_json_extract('src', ['acmc_2'], type='float', alias='acmc_2') }},
        {{ trx_json_extract('src', ['acmc_3'], type='float', alias='acmc_3') }},
        {{ trx_json_extract('src', ['acmt'], type='float', alias='acmt') }},
        {{ trx_json_extract('src', ['agrp'], type='varchar', alias='agrp') }},
        {{ trx_json_extract('src', ['amnt_1'], type='float', alias='amnt_1') }},
        {{ trx_json_extract('src', ['amnt_2'], type='float', alias='amnt_2') }},
        {{ trx_json_extract('src', ['amnt_3'], type='float', alias='amnt_3') }},
        {{ trx_json_extract('src', ['amtt'], type='float', alias='amtt') }},
        {{ trx_json_extract('src', ['ctgy'], type='varchar', alias='ctgy') }},
        {{ trx_json_extract('src', ['curr'], type='varchar', alias='curr') }},
        {{ trx_json_extract('src', ['jobn'], type='int', alias='jobn') }},
        {{ trx_json_extract('src', ['ltdd_1'], type='float', alias='ltdd_1') }},
        {{ trx_json_extract('src', ['ltdd_2'], type='float', alias='ltdd_2') }},
        {{ trx_json_extract('src', ['ltdd_3'], type='float', alias='ltdd_3') }},
        {{ trx_json_extract('src', ['ratd'], type='timestamp_ntz', alias='ratd') }},
        {{ trx_json_extract('src', ['rate_1'], type='float', alias='rate_1') }},
        {{ trx_json_extract('src', ['rate_2'], type='float', alias='rate_2') }},
        {{ trx_json_extract('src', ['rate_3'], type='float', alias='rate_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['reas'], type='varchar', alias='reas') }},
        {{ trx_json_extract('src', ['sctg'], type='varchar', alias='sctg') }},
        {{ trx_json_extract('src', ['tagn', 'en_US'], type='varchar', alias='tagn__en_us') }},
        {{ trx_json_extract('src', ['trid'], type='int', alias='trid') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['xaex'], type='varchar', alias='xaex') }},
        {{ trx_json_extract('src', ['xanb'], type='varchar', alias='xanb') }},
        {{ trx_json_extract('src', ['xcom'], type='int', alias='xcom') }},
        {{ trx_json_extract('src', ['rcst_1'], type='float', alias='rcst_1') }},
        {{ trx_json_extract('src', ['rcst_2'], type='float', alias='rcst_2') }},
        {{ trx_json_extract('src', ['rcst_3'], type='float', alias='rcst_3') }},
        {{ trx_json_extract('src', ['ltdr_1'], type='float', alias='ltdr_1') }},
        {{ trx_json_extract('src', ['ltdr_2'], type='float', alias='ltdr_2') }},
        {{ trx_json_extract('src', ['ltdr_3'], type='float', alias='ltdr_3') }},
        {{ trx_json_extract('src', ['tkey_ref_compnr'], type='int', alias='tkey_ref_compnr') }},
        {{ trx_json_extract('src', ['agrp_ref_compnr'], type='int', alias='agrp_ref_compnr') }},
        {{ trx_json_extract('src', ['ctgy_sctg_ref_compnr'], type='int', alias='ctgy_sctg_ref_compnr') }},
        {{ trx_json_extract('src', ['ctgy_ref_compnr'], type='int', alias='ctgy_ref_compnr') }},
        {{ trx_json_extract('src', ['reas_ref_compnr'], type='int', alias='reas_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam810') }}
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
