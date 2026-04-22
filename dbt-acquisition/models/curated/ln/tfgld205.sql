{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld205 History - Dimension/Ledger Account Combination Totals table, Generated 2026-04-10T19:41:44Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cono'], type='int', alias='cono') }},
        {{ trx_json_extract('src', ['ptyp'], type='int', alias='ptyp') }},
        {{ trx_json_extract('src', ['ptyp_kw'], type='varchar', alias='ptyp_kw') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['prno'], type='int', alias='prno') }},
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
        {{ trx_json_extract('src', ['fdam'], type='float', alias='fdam') }},
        {{ trx_json_extract('src', ['fcam'], type='float', alias='fcam') }},
        {{ trx_json_extract('src', ['fdah_1'], type='float', alias='fdah_1') }},
        {{ trx_json_extract('src', ['fdah_2'], type='float', alias='fdah_2') }},
        {{ trx_json_extract('src', ['fdah_3'], type='float', alias='fdah_3') }},
        {{ trx_json_extract('src', ['fcah_1'], type='float', alias='fcah_1') }},
        {{ trx_json_extract('src', ['fcah_2'], type='float', alias='fcah_2') }},
        {{ trx_json_extract('src', ['fcah_3'], type='float', alias='fcah_3') }},
        {{ trx_json_extract('src', ['fqt1'], type='float', alias='fqt1') }},
        {{ trx_json_extract('src', ['fqt2'], type='float', alias='fqt2') }},
        {{ trx_json_extract('src', ['ndam'], type='float', alias='ndam') }},
        {{ trx_json_extract('src', ['ncam'], type='float', alias='ncam') }},
        {{ trx_json_extract('src', ['ndah_1'], type='float', alias='ndah_1') }},
        {{ trx_json_extract('src', ['ndah_2'], type='float', alias='ndah_2') }},
        {{ trx_json_extract('src', ['ndah_3'], type='float', alias='ndah_3') }},
        {{ trx_json_extract('src', ['ncah_1'], type='float', alias='ncah_1') }},
        {{ trx_json_extract('src', ['ncah_2'], type='float', alias='ncah_2') }},
        {{ trx_json_extract('src', ['ncah_3'], type='float', alias='ncah_3') }},
        {{ trx_json_extract('src', ['nqt1'], type='float', alias='nqt1') }},
        {{ trx_json_extract('src', ['nqt2'], type='float', alias='nqt2') }},
        {{ trx_json_extract('src', ['olap'], type='int', alias='olap') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['fdah_rfrc'], type='float', alias='fdah_rfrc') }},
        {{ trx_json_extract('src', ['fdah_dtwc'], type='float', alias='fdah_dtwc') }},
        {{ trx_json_extract('src', ['fcah_rfrc'], type='float', alias='fcah_rfrc') }},
        {{ trx_json_extract('src', ['fcah_dtwc'], type='float', alias='fcah_dtwc') }},
        {{ trx_json_extract('src', ['ndah_rfrc'], type='float', alias='ndah_rfrc') }},
        {{ trx_json_extract('src', ['ndah_dtwc'], type='float', alias='ndah_dtwc') }},
        {{ trx_json_extract('src', ['ncah_rfrc'], type='float', alias='ncah_rfrc') }},
        {{ trx_json_extract('src', ['ncah_dtwc'], type='float', alias='ncah_dtwc') }},
        {{ trx_json_extract('src', ['dimx_sgm1'], type='varchar', alias='dimx_sgm1') }},
        {{ trx_json_extract('src', ['dimx_sgm2'], type='varchar', alias='dimx_sgm2') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld205') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cono', 'ptyp', 'year', 'prno', 'dim1', 'dim2', 'dim3', 'dim4', 'dim5', 'dims', 'leac', 'ccur', 'duac', 'bpid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
