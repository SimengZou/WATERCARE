{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffst121 Statement Ledger/Dimension Accounts table, Generated 2026-04-10T19:41:39Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['fstm'], type='varchar', alias='fstm') }},
        {{ trx_json_extract('src', ['accn'], type='varchar', alias='accn') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['cmpm'], type='varchar', alias='cmpm') }},
        {{ trx_json_extract('src', ['cmpf'], type='int', alias='cmpf') }},
        {{ trx_json_extract('src', ['cmpt'], type='int', alias='cmpt') }},
        {{ trx_json_extract('src', ['lacm'], type='varchar', alias='lacm') }},
        {{ trx_json_extract('src', ['lacf'], type='varchar', alias='lacf') }},
        {{ trx_json_extract('src', ['lact'], type='varchar', alias='lact') }},
        {{ trx_json_extract('src', ['dimm_1'], type='varchar', alias='dimm_1') }},
        {{ trx_json_extract('src', ['dimm_2'], type='varchar', alias='dimm_2') }},
        {{ trx_json_extract('src', ['dimm_3'], type='varchar', alias='dimm_3') }},
        {{ trx_json_extract('src', ['dimm_4'], type='varchar', alias='dimm_4') }},
        {{ trx_json_extract('src', ['dimm_5'], type='varchar', alias='dimm_5') }},
        {{ trx_json_extract('src', ['dimm_6'], type='varchar', alias='dimm_6') }},
        {{ trx_json_extract('src', ['dimm_7'], type='varchar', alias='dimm_7') }},
        {{ trx_json_extract('src', ['dimm_8'], type='varchar', alias='dimm_8') }},
        {{ trx_json_extract('src', ['dimm_9'], type='varchar', alias='dimm_9') }},
        {{ trx_json_extract('src', ['dimm_10'], type='varchar', alias='dimm_10') }},
        {{ trx_json_extract('src', ['dimm_11'], type='varchar', alias='dimm_11') }},
        {{ trx_json_extract('src', ['dimm_12'], type='varchar', alias='dimm_12') }},
        {{ trx_json_extract('src', ['dimf_1'], type='varchar', alias='dimf_1') }},
        {{ trx_json_extract('src', ['dimf_2'], type='varchar', alias='dimf_2') }},
        {{ trx_json_extract('src', ['dimf_3'], type='varchar', alias='dimf_3') }},
        {{ trx_json_extract('src', ['dimf_4'], type='varchar', alias='dimf_4') }},
        {{ trx_json_extract('src', ['dimf_5'], type='varchar', alias='dimf_5') }},
        {{ trx_json_extract('src', ['dimf_6'], type='varchar', alias='dimf_6') }},
        {{ trx_json_extract('src', ['dimf_7'], type='varchar', alias='dimf_7') }},
        {{ trx_json_extract('src', ['dimf_8'], type='varchar', alias='dimf_8') }},
        {{ trx_json_extract('src', ['dimf_9'], type='varchar', alias='dimf_9') }},
        {{ trx_json_extract('src', ['dimf_10'], type='varchar', alias='dimf_10') }},
        {{ trx_json_extract('src', ['dimf_11'], type='varchar', alias='dimf_11') }},
        {{ trx_json_extract('src', ['dimf_12'], type='varchar', alias='dimf_12') }},
        {{ trx_json_extract('src', ['dimt_1'], type='varchar', alias='dimt_1') }},
        {{ trx_json_extract('src', ['dimt_2'], type='varchar', alias='dimt_2') }},
        {{ trx_json_extract('src', ['dimt_3'], type='varchar', alias='dimt_3') }},
        {{ trx_json_extract('src', ['dimt_4'], type='varchar', alias='dimt_4') }},
        {{ trx_json_extract('src', ['dimt_5'], type='varchar', alias='dimt_5') }},
        {{ trx_json_extract('src', ['dimt_6'], type='varchar', alias='dimt_6') }},
        {{ trx_json_extract('src', ['dimt_7'], type='varchar', alias='dimt_7') }},
        {{ trx_json_extract('src', ['dimt_8'], type='varchar', alias='dimt_8') }},
        {{ trx_json_extract('src', ['dimt_9'], type='varchar', alias='dimt_9') }},
        {{ trx_json_extract('src', ['dimt_10'], type='varchar', alias='dimt_10') }},
        {{ trx_json_extract('src', ['dimt_11'], type='varchar', alias='dimt_11') }},
        {{ trx_json_extract('src', ['dimt_12'], type='varchar', alias='dimt_12') }},
        {{ trx_json_extract('src', ['fstm_accn_ref_compnr'], type='int', alias='fstm_accn_ref_compnr') }},
        {{ trx_json_extract('src', ['fstm_ref_compnr'], type='int', alias='fstm_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffst121') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'fstm', 'accn', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
