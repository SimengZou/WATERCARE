{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfacp600 Open Items Payment-related Documents A/P table, Generated 2026-04-10T19:41:31Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['pcom'], type='int', alias='pcom') }},
        {{ trx_json_extract('src', ['payt'], type='varchar', alias='payt') }},
        {{ trx_json_extract('src', ['payd'], type='int', alias='payd') }},
        {{ trx_json_extract('src', ['payl'], type='int', alias='payl') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['sdat'], type='timestamp_ntz', alias='sdat') }},
        {{ trx_json_extract('src', ['pyid'], type='varchar', alias='pyid') }},
        {{ trx_json_extract('src', ['ipco'], type='int', alias='ipco') }},
        {{ trx_json_extract('src', ['ipty'], type='varchar', alias='ipty') }},
        {{ trx_json_extract('src', ['ipdo'], type='int', alias='ipdo') }},
        {{ trx_json_extract('src', ['ipli'], type='int', alias='ipli') }},
        {{ trx_json_extract('src', ['tpay'], type='int', alias='tpay') }},
        {{ trx_json_extract('src', ['tpay_kw'], type='varchar', alias='tpay_kw') }},
        {{ trx_json_extract('src', ['pbcp'], type='int', alias='pbcp') }},
        {{ trx_json_extract('src', ['pbtn'], type='int', alias='pbtn') }},
        {{ trx_json_extract('src', ['paym'], type='varchar', alias='paym') }},
        {{ trx_json_extract('src', ['bank'], type='varchar', alias='bank') }},
        {{ trx_json_extract('src', ['basu'], type='varchar', alias='basu') }},
        {{ trx_json_extract('src', ['ptbp'], type='varchar', alias='ptbp') }},
        {{ trx_json_extract('src', ['tpbp'], type='varchar', alias='tpbp') }},
        {{ trx_json_extract('src', ['ddat'], type='date', alias='ddat') }},
        {{ trx_json_extract('src', ['amnt'], type='float', alias='amnt') }},
        {{ trx_json_extract('src', ['curr'], type='varchar', alias='curr') }},
        {{ trx_json_extract('src', ['amtl'], type='float', alias='amtl') }},
        {{ trx_json_extract('src', ['step'], type='int', alias='step') }},
        {{ trx_json_extract('src', ['step_kw'], type='varchar', alias='step_kw') }},
        {{ trx_json_extract('src', ['gcom'], type='int', alias='gcom') }},
        {{ trx_json_extract('src', ['gtyp'], type='varchar', alias='gtyp') }},
        {{ trx_json_extract('src', ['gdoc'], type='int', alias='gdoc') }},
        {{ trx_json_extract('src', ['glin'], type='int', alias='glin') }},
        {{ trx_json_extract('src', ['reas'], type='varchar', alias='reas') }},
        {{ trx_json_extract('src', ['usrc'], type='varchar', alias='usrc') }},
        {{ trx_json_extract('src', ['ptbp_ref_compnr'], type='int', alias='ptbp_ref_compnr') }},
        {{ trx_json_extract('src', ['tpbp_ref_compnr'], type='int', alias='tpbp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfacp600') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'pcom', 'payt', 'payd', 'payl', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
