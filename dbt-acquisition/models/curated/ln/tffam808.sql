{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam808 Periodic Book Value Summary table, Generated 2026-04-10T19:41:35Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['anbr'], type='varchar', alias='anbr') }},
        {{ trx_json_extract('src', ['aext'], type='varchar', alias='aext') }},
        {{ trx_json_extract('src', ['book'], type='varchar', alias='book') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['prod'], type='int', alias='prod') }},
        {{ trx_json_extract('src', ['cost_1'], type='float', alias='cost_1') }},
        {{ trx_json_extract('src', ['cost_2'], type='float', alias='cost_2') }},
        {{ trx_json_extract('src', ['cost_3'], type='float', alias='cost_3') }},
        {{ trx_json_extract('src', ['ocst_1'], type='float', alias='ocst_1') }},
        {{ trx_json_extract('src', ['ocst_2'], type='float', alias='ocst_2') }},
        {{ trx_json_extract('src', ['ocst_3'], type='float', alias='ocst_3') }},
        {{ trx_json_extract('src', ['ltdd_1'], type='float', alias='ltdd_1') }},
        {{ trx_json_extract('src', ['ltdd_2'], type='float', alias='ltdd_2') }},
        {{ trx_json_extract('src', ['ltdd_3'], type='float', alias='ltdd_3') }},
        {{ trx_json_extract('src', ['s179_1'], type='float', alias='s179_1') }},
        {{ trx_json_extract('src', ['s179_2'], type='float', alias='s179_2') }},
        {{ trx_json_extract('src', ['s179_3'], type='float', alias='s179_3') }},
        {{ trx_json_extract('src', ['salv_1'], type='float', alias='salv_1') }},
        {{ trx_json_extract('src', ['salv_2'], type='float', alias='salv_2') }},
        {{ trx_json_extract('src', ['salv_3'], type='float', alias='salv_3') }},
        {{ trx_json_extract('src', ['ytdd_1'], type='float', alias='ytdd_1') }},
        {{ trx_json_extract('src', ['ytdd_2'], type='float', alias='ytdd_2') }},
        {{ trx_json_extract('src', ['ytdd_3'], type='float', alias='ytdd_3') }},
        {{ trx_json_extract('src', ['susp'], type='int', alias='susp') }},
        {{ trx_json_extract('src', ['susp_kw'], type='varchar', alias='susp_kw') }},
        {{ trx_json_extract('src', ['rcst_1'], type='float', alias='rcst_1') }},
        {{ trx_json_extract('src', ['rcst_2'], type='float', alias='rcst_2') }},
        {{ trx_json_extract('src', ['rcst_3'], type='float', alias='rcst_3') }},
        {{ trx_json_extract('src', ['ltdr_1'], type='float', alias='ltdr_1') }},
        {{ trx_json_extract('src', ['ltdr_2'], type='float', alias='ltdr_2') }},
        {{ trx_json_extract('src', ['ltdr_3'], type='float', alias='ltdr_3') }},
        {{ trx_json_extract('src', ['ytdr_1'], type='float', alias='ytdr_1') }},
        {{ trx_json_extract('src', ['ytdr_2'], type='float', alias='ytdr_2') }},
        {{ trx_json_extract('src', ['ytdr_3'], type='float', alias='ytdr_3') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam808') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'anbr', 'aext', 'book', 'year', 'prod']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
