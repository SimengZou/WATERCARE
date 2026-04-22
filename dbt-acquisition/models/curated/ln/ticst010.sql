{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN ticst010 End Item Unit Costs table, Generated 2026-04-10T19:41:47Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['pdno'], type='varchar', alias='pdno') }},
        {{ trx_json_extract('src', ['cstv'], type='int', alias='cstv') }},
        {{ trx_json_extract('src', ['cstv_kw'], type='varchar', alias='cstv_kw') }},
        {{ trx_json_extract('src', ['cwoc'], type='varchar', alias='cwoc') }},
        {{ trx_json_extract('src', ['cpcp'], type='varchar', alias='cpcp') }},
        {{ trx_json_extract('src', ['addc'], type='int', alias='addc') }},
        {{ trx_json_extract('src', ['addc_kw'], type='varchar', alias='addc_kw') }},
        {{ trx_json_extract('src', ['csto'], type='int', alias='csto') }},
        {{ trx_json_extract('src', ['csto_kw'], type='varchar', alias='csto_kw') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['nune'], type='float', alias='nune') }},
        {{ trx_json_extract('src', ['eamt_1'], type='float', alias='eamt_1') }},
        {{ trx_json_extract('src', ['eamt_2'], type='float', alias='eamt_2') }},
        {{ trx_json_extract('src', ['eamt_3'], type='float', alias='eamt_3') }},
        {{ trx_json_extract('src', ['nuna'], type='float', alias='nuna') }},
        {{ trx_json_extract('src', ['aamt_1'], type='float', alias='aamt_1') }},
        {{ trx_json_extract('src', ['aamt_2'], type='float', alias='aamt_2') }},
        {{ trx_json_extract('src', ['aamt_3'], type='float', alias='aamt_3') }},
        {{ trx_json_extract('src', ['pdno_ref_compnr'], type='int', alias='pdno_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcp_ref_compnr'], type='int', alias='cpcp_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['eamt_lclc'], type='float', alias='eamt_lclc') }},
        {{ trx_json_extract('src', ['eamt_rpc1'], type='float', alias='eamt_rpc1') }},
        {{ trx_json_extract('src', ['eamt_rpc2'], type='float', alias='eamt_rpc2') }},
        {{ trx_json_extract('src', ['eamt_refc'], type='float', alias='eamt_refc') }},
        {{ trx_json_extract('src', ['eamt_dwhc'], type='float', alias='eamt_dwhc') }},
        {{ trx_json_extract('src', ['aamt_lclc'], type='float', alias='aamt_lclc') }},
        {{ trx_json_extract('src', ['aamt_rpc1'], type='float', alias='aamt_rpc1') }},
        {{ trx_json_extract('src', ['aamt_rpc2'], type='float', alias='aamt_rpc2') }},
        {{ trx_json_extract('src', ['aamt_refc'], type='float', alias='aamt_refc') }},
        {{ trx_json_extract('src', ['aamt_dwhc'], type='float', alias='aamt_dwhc') }},
        {{ trx_json_extract('src', ['dptm_fcmp'], type='int', alias='dptm_fcmp') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_ticst010') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'pdno', 'cstv', 'cwoc', 'cpcp', 'addc', 'csto']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
