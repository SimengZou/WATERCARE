{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfacp270 Purchase Order Related Financial Transactions table, Generated 2026-04-10T19:41:30Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['guid'], type='varchar', alias='guid') }},
        {{ trx_json_extract('src', ['dbcr'], type='int', alias='dbcr') }},
        {{ trx_json_extract('src', ['dbcr_kw'], type='varchar', alias='dbcr_kw') }},
        {{ trx_json_extract('src', ['idtc'], type='varchar', alias='idtc') }},
        {{ trx_json_extract('src', ['ktrn'], type='int', alias='ktrn') }},
        {{ trx_json_extract('src', ['ktrn_kw'], type='varchar', alias='ktrn_kw') }},
        {{ trx_json_extract('src', ['reco'], type='int', alias='reco') }},
        {{ trx_json_extract('src', ['reco_kw'], type='varchar', alias='reco_kw') }},
        {{ trx_json_extract('src', ['recs'], type='int', alias='recs') }},
        {{ trx_json_extract('src', ['rbon'], type='varchar', alias='rbon') }},
        {{ trx_json_extract('src', ['rbid'], type='varchar', alias='rbid') }},
        {{ trx_json_extract('src', ['rpon'], type='int', alias='rpon') }},
        {{ trx_json_extract('src', ['obre'], type='varchar', alias='obre') }},
        {{ trx_json_extract('src', ['buid'], type='varchar', alias='buid') }},
        {{ trx_json_extract('src', ['txin'], type='int', alias='txin') }},
        {{ trx_json_extract('src', ['txin_kw'], type='varchar', alias='txin_kw') }},
        {{ trx_json_extract('src', ['ocmp'], type='int', alias='ocmp') }},
        {{ trx_json_extract('src', ['fcom'], type='int', alias='fcom') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['nuni'], type='float', alias='nuni') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['amnt'], type='float', alias='amnt') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['amth_1'], type='float', alias='amth_1') }},
        {{ trx_json_extract('src', ['amth_2'], type='float', alias='amth_2') }},
        {{ trx_json_extract('src', ['amth_3'], type='float', alias='amth_3') }},
        {{ trx_json_extract('src', ['dcom'], type='int', alias='dcom') }},
        {{ trx_json_extract('src', ['ttyp'], type='varchar', alias='ttyp') }},
        {{ trx_json_extract('src', ['docn'], type='int', alias='docn') }},
        {{ trx_json_extract('src', ['clss'], type='int', alias='clss') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfacp270') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'guid', 'dbcr']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
