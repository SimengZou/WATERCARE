{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam805 Transaction Distribution table, Generated 2026-04-10T19:41:34Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['anbr'], type='varchar', alias='anbr') }},
        {{ trx_json_extract('src', ['aext'], type='varchar', alias='aext') }},
        {{ trx_json_extract('src', ['book'], type='varchar', alias='book') }},
        {{ trx_json_extract('src', ['tkey'], type='int', alias='tkey') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['lkey'], type='int', alias='lkey') }},
        {{ trx_json_extract('src', ['cost_1'], type='float', alias='cost_1') }},
        {{ trx_json_extract('src', ['cost_2'], type='float', alias='cost_2') }},
        {{ trx_json_extract('src', ['cost_3'], type='float', alias='cost_3') }},
        {{ trx_json_extract('src', ['depr_1'], type='float', alias='depr_1') }},
        {{ trx_json_extract('src', ['depr_2'], type='float', alias='depr_2') }},
        {{ trx_json_extract('src', ['depr_3'], type='float', alias='depr_3') }},
        {{ trx_json_extract('src', ['rcst_1'], type='float', alias='rcst_1') }},
        {{ trx_json_extract('src', ['rcst_2'], type='float', alias='rcst_2') }},
        {{ trx_json_extract('src', ['rcst_3'], type='float', alias='rcst_3') }},
        {{ trx_json_extract('src', ['rdpr_1'], type='float', alias='rdpr_1') }},
        {{ trx_json_extract('src', ['rdpr_2'], type='float', alias='rdpr_2') }},
        {{ trx_json_extract('src', ['rdpr_3'], type='float', alias='rdpr_3') }},
        {{ trx_json_extract('src', ['tkey_ref_compnr'], type='int', alias='tkey_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam805') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'anbr', 'aext', 'book', 'tkey', 'bpid', 'lkey']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
