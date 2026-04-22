{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam830 Depreciation Transaction table, Generated 2026-04-10T19:41:36Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['tkey'], type='int', alias='tkey') }},
        {{ trx_json_extract('src', ['abdf'], type='int', alias='abdf') }},
        {{ trx_json_extract('src', ['abdf_kw'], type='varchar', alias='abdf_kw') }},
        {{ trx_json_extract('src', ['cost_1'], type='float', alias='cost_1') }},
        {{ trx_json_extract('src', ['cost_2'], type='float', alias='cost_2') }},
        {{ trx_json_extract('src', ['cost_3'], type='float', alias='cost_3') }},
        {{ trx_json_extract('src', ['curd'], type='date', alias='curd') }},
        {{ trx_json_extract('src', ['depr_1'], type='float', alias='depr_1') }},
        {{ trx_json_extract('src', ['depr_2'], type='float', alias='depr_2') }},
        {{ trx_json_extract('src', ['depr_3'], type='float', alias='depr_3') }},
        {{ trx_json_extract('src', ['dtty'], type='int', alias='dtty') }},
        {{ trx_json_extract('src', ['dtty_kw'], type='varchar', alias='dtty_kw') }},
        {{ trx_json_extract('src', ['jobn'], type='int', alias='jobn') }},
        {{ trx_json_extract('src', ['last'], type='date', alias='last') }},
        {{ trx_json_extract('src', ['ltdd_1'], type='float', alias='ltdd_1') }},
        {{ trx_json_extract('src', ['ltdd_2'], type='float', alias='ltdd_2') }},
        {{ trx_json_extract('src', ['ltdd_3'], type='float', alias='ltdd_3') }},
        {{ trx_json_extract('src', ['ltdu'], type='int', alias='ltdu') }},
        {{ trx_json_extract('src', ['meth'], type='varchar', alias='meth') }},
        {{ trx_json_extract('src', ['prop'], type='varchar', alias='prop') }},
        {{ trx_json_extract('src', ['susp'], type='int', alias='susp') }},
        {{ trx_json_extract('src', ['susp_kw'], type='varchar', alias='susp_kw') }},
        {{ trx_json_extract('src', ['unit'], type='int', alias='unit') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['ytdd_1'], type='float', alias='ytdd_1') }},
        {{ trx_json_extract('src', ['ytdd_2'], type='float', alias='ytdd_2') }},
        {{ trx_json_extract('src', ['ytdd_3'], type='float', alias='ytdd_3') }},
        {{ trx_json_extract('src', ['adpc'], type='float', alias='adpc') }},
        {{ trx_json_extract('src', ['rcst_1'], type='float', alias='rcst_1') }},
        {{ trx_json_extract('src', ['rcst_2'], type='float', alias='rcst_2') }},
        {{ trx_json_extract('src', ['rcst_3'], type='float', alias='rcst_3') }},
        {{ trx_json_extract('src', ['ltdr_1'], type='float', alias='ltdr_1') }},
        {{ trx_json_extract('src', ['ltdr_2'], type='float', alias='ltdr_2') }},
        {{ trx_json_extract('src', ['ltdr_3'], type='float', alias='ltdr_3') }},
        {{ trx_json_extract('src', ['ytdr_1'], type='float', alias='ytdr_1') }},
        {{ trx_json_extract('src', ['ytdr_2'], type='float', alias='ytdr_2') }},
        {{ trx_json_extract('src', ['ytdr_3'], type='float', alias='ytdr_3') }},
        {{ trx_json_extract('src', ['tkey_ref_compnr'], type='int', alias='tkey_ref_compnr') }},
        {{ trx_json_extract('src', ['meth_ref_compnr'], type='int', alias='meth_ref_compnr') }},
        {{ trx_json_extract('src', ['prop_ref_compnr'], type='int', alias='prop_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam830') }}
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
