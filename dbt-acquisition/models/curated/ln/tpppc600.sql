{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpppc600 Overhead Cost Transactions table, Generated 2026-04-10T19:42:17Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['base'], type='varchar', alias='base') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['cotp'], type='int', alias='cotp') }},
        {{ trx_json_extract('src', ['cotp_kw'], type='varchar', alias='cotp_kw') }},
        {{ trx_json_extract('src', ['coob'], type='varchar', alias='coob') }},
        {{ trx_json_extract('src', ['sern'], type='int', alias='sern') }},
        {{ trx_json_extract('src', ['ohcs'], type='int', alias='ohcs') }},
        {{ trx_json_extract('src', ['tcob'], type='varchar', alias='tcob') }},
        {{ trx_json_extract('src', ['cccp'], type='varchar', alias='cccp') }},
        {{ trx_json_extract('src', ['vrsn'], type='int', alias='vrsn') }},
        {{ trx_json_extract('src', ['btyp'], type='int', alias='btyp') }},
        {{ trx_json_extract('src', ['btyp_kw'], type='varchar', alias='btyp_kw') }},
        {{ trx_json_extract('src', ['amnt'], type='float', alias='amnt') }},
        {{ trx_json_extract('src', ['hour'], type='float', alias='hour') }},
        {{ trx_json_extract('src', ['unit'], type='varchar', alias='unit') }},
        {{ trx_json_extract('src', ['perc'], type='float', alias='perc') }},
        {{ trx_json_extract('src', ['rate'], type='float', alias='rate') }},
        {{ trx_json_extract('src', ['levl'], type='int', alias='levl') }},
        {{ trx_json_extract('src', ['tbap'], type='float', alias='tbap') }},
        {{ trx_json_extract('src', ['calc'], type='float', alias='calc') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['bamt'], type='float', alias='bamt') }},
        {{ trx_json_extract('src', ['bprc'], type='float', alias='bprc') }},
        {{ trx_json_extract('src', ['brte'], type='float', alias='brte') }},
        {{ trx_json_extract('src', ['btba'], type='float', alias='btba') }},
        {{ trx_json_extract('src', ['bclc'], type='float', alias='bclc') }},
        {{ trx_json_extract('src', ['bcur'], type='varchar', alias='bcur') }},
        {{ trx_json_extract('src', ['ohst'], type='int', alias='ohst') }},
        {{ trx_json_extract('src', ['ohst_kw'], type='varchar', alias='ohst_kw') }},
        {{ trx_json_extract('src', ['appl'], type='int', alias='appl') }},
        {{ trx_json_extract('src', ['appl_kw'], type='varchar', alias='appl_kw') }},
        {{ trx_json_extract('src', ['ctdt'], type='timestamp_ntz', alias='ctdt') }},
        {{ trx_json_extract('src', ['trdt'], type='timestamp_ntz', alias='trdt') }},
        {{ trx_json_extract('src', ['cldt'], type='timestamp_ntz', alias='cldt') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['base_vrsn_ref_compnr'], type='int', alias='base_vrsn_ref_compnr') }},
        {{ trx_json_extract('src', ['tcob_ref_compnr'], type='int', alias='tcob_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpppc600') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'user', 'cprj', 'base', 'cspa', 'cact', 'cotp', 'coob', 'sern', 'ohcs']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
