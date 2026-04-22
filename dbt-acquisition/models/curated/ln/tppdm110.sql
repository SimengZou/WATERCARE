{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppdm110 Standard Activities table, Generated 2026-04-10T19:41:58Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['tact'], type='int', alias='tact') }},
        {{ trx_json_extract('src', ['tact_kw'], type='varchar', alias='tact_kw') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['cuti'], type='varchar', alias='cuti') }},
        {{ trx_json_extract('src', ['prsp'], type='float', alias='prsp') }},
        {{ trx_json_extract('src', ['lvps'], type='int', alias='lvps') }},
        {{ trx_json_extract('src', ['lvps_kw'], type='varchar', alias='lvps_kw') }},
        {{ trx_json_extract('src', ['prin'], type='int', alias='prin') }},
        {{ trx_json_extract('src', ['prin_kw'], type='varchar', alias='prin_kw') }},
        {{ trx_json_extract('src', ['cspt'], type='int', alias='cspt') }},
        {{ trx_json_extract('src', ['cspt_kw'], type='varchar', alias='cspt_kw') }},
        {{ trx_json_extract('src', ['setl'], type='int', alias='setl') }},
        {{ trx_json_extract('src', ['setl_kw'], type='varchar', alias='setl_kw') }},
        {{ trx_json_extract('src', ['blbl'], type='int', alias='blbl') }},
        {{ trx_json_extract('src', ['blbl_kw'], type='varchar', alias='blbl_kw') }},
        {{ trx_json_extract('src', ['mevl'], type='int', alias='mevl') }},
        {{ trx_json_extract('src', ['mevl_kw'], type='varchar', alias='mevl_kw') }},
        {{ trx_json_extract('src', ['capt'], type='int', alias='capt') }},
        {{ trx_json_extract('src', ['capt_kw'], type='varchar', alias='capt_kw') }},
        {{ trx_json_extract('src', ['rfac'], type='varchar', alias='rfac') }},
        {{ trx_json_extract('src', ['freq'], type='float', alias='freq') }},
        {{ trx_json_extract('src', ['afrt'], type='int', alias='afrt') }},
        {{ trx_json_extract('src', ['afrt_kw'], type='varchar', alias='afrt_kw') }},
        {{ trx_json_extract('src', ['prst'], type='float', alias='prst') }},
        {{ trx_json_extract('src', ['prnd'], type='float', alias='prnd') }},
        {{ trx_json_extract('src', ['prms'], type='float', alias='prms') }},
        {{ trx_json_extract('src', ['eoty'], type='int', alias='eoty') }},
        {{ trx_json_extract('src', ['eoty_kw'], type='varchar', alias='eoty_kw') }},
        {{ trx_json_extract('src', ['plmc'], type='int', alias='plmc') }},
        {{ trx_json_extract('src', ['pbpt', 'en_US'], type='varchar', alias='pbpt__en_us') }},
        {{ trx_json_extract('src', ['plmp'], type='varchar', alias='plmp') }},
        {{ trx_json_extract('src', ['rout'], type='varchar', alias='rout') }},
        {{ trx_json_extract('src', ['rent'], type='int', alias='rent') }},
        {{ trx_json_extract('src', ['rent_kw'], type='varchar', alias='rent_kw') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cuni_ref_compnr'], type='int', alias='cuni_ref_compnr') }},
        {{ trx_json_extract('src', ['cuti_ref_compnr'], type='int', alias='cuti_ref_compnr') }},
        {{ trx_json_extract('src', ['plmc_ref_compnr'], type='int', alias='plmc_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppdm110') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cact']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
