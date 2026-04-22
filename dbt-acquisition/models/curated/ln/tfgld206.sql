{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld206 Opening Balances - Combination of Dimension/Ledger/Currency table, Generated 2026-04-10T19:41:45Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cono'], type='int', alias='cono') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['dim1'], type='varchar', alias='dim1') }},
        {{ trx_json_extract('src', ['dim2'], type='varchar', alias='dim2') }},
        {{ trx_json_extract('src', ['dim3'], type='varchar', alias='dim3') }},
        {{ trx_json_extract('src', ['dim4'], type='varchar', alias='dim4') }},
        {{ trx_json_extract('src', ['dim5'], type='varchar', alias='dim5') }},
        {{ trx_json_extract('src', ['dim6'], type='varchar', alias='dim6') }},
        {{ trx_json_extract('src', ['dim7'], type='varchar', alias='dim7') }},
        {{ trx_json_extract('src', ['dim8'], type='varchar', alias='dim8') }},
        {{ trx_json_extract('src', ['dim9'], type='varchar', alias='dim9') }},
        {{ trx_json_extract('src', ['dm10'], type='varchar', alias='dm10') }},
        {{ trx_json_extract('src', ['dm11'], type='varchar', alias='dm11') }},
        {{ trx_json_extract('src', ['dm12'], type='varchar', alias='dm12') }},
        {{ trx_json_extract('src', ['dims'], type='varchar', alias='dims') }},
        {{ trx_json_extract('src', ['leac'], type='varchar', alias='leac') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['duac'], type='int', alias='duac') }},
        {{ trx_json_extract('src', ['duac_kw'], type='varchar', alias='duac_kw') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['ftob'], type='float', alias='ftob') }},
        {{ trx_json_extract('src', ['fobh_1'], type='float', alias='fobh_1') }},
        {{ trx_json_extract('src', ['fobh_2'], type='float', alias='fobh_2') }},
        {{ trx_json_extract('src', ['fobh_3'], type='float', alias='fobh_3') }},
        {{ trx_json_extract('src', ['ntob'], type='float', alias='ntob') }},
        {{ trx_json_extract('src', ['nobh_1'], type='float', alias='nobh_1') }},
        {{ trx_json_extract('src', ['nobh_2'], type='float', alias='nobh_2') }},
        {{ trx_json_extract('src', ['nobh_3'], type='float', alias='nobh_3') }},
        {{ trx_json_extract('src', ['qob1'], type='float', alias='qob1') }},
        {{ trx_json_extract('src', ['qob2'], type='float', alias='qob2') }},
        {{ trx_json_extract('src', ['nob1'], type='float', alias='nob1') }},
        {{ trx_json_extract('src', ['nob2'], type='float', alias='nob2') }},
        {{ trx_json_extract('src', ['olap'], type='int', alias='olap') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['fobh_rfrc'], type='float', alias='fobh_rfrc') }},
        {{ trx_json_extract('src', ['fobh_dtwc'], type='float', alias='fobh_dtwc') }},
        {{ trx_json_extract('src', ['nobh_rfrc'], type='float', alias='nobh_rfrc') }},
        {{ trx_json_extract('src', ['nobh_dtwc'], type='float', alias='nobh_dtwc') }},
        {{ trx_json_extract('src', ['dimx_sgm1'], type='varchar', alias='dimx_sgm1') }},
        {{ trx_json_extract('src', ['dimx_sgm2'], type='varchar', alias='dimx_sgm2') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld206') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cono', 'year', 'dim1', 'dim2', 'dim3', 'dim4', 'dim5', 'dims', 'leac', 'ccur', 'duac', 'bpid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
