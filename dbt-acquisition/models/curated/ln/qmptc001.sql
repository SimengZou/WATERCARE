{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN qmptc001 Characteristics table, Generated 2022-06-15T01:21:04Z from package combination ce01055',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['char'], type='varchar', alias='char') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['ctyp'], type='int', alias='ctyp') }},
        {{ trx_json_extract('src', ['ctyp_kw'], type='varchar', alias='ctyp_kw') }},
        {{ trx_json_extract('src', ['mthd'], type='int', alias='mthd') }},
        {{ trx_json_extract('src', ['mthd_kw'], type='varchar', alias='mthd_kw') }},
        {{ trx_json_extract('src', ['algo'], type='varchar', alias='algo') }},
        {{ trx_json_extract('src', ['cstd', 'en_US'], type='varchar', alias='cstd__en_us') }},
        {{ trx_json_extract('src', ['dtst'], type='varchar', alias='dtst') }},
        {{ trx_json_extract('src', ['chun'], type='varchar', alias='chun') }},
        {{ trx_json_extract('src', ['fcvl'], type='float', alias='fcvl') }},
        {{ trx_json_extract('src', ['falg'], type='int', alias='falg') }},
        {{ trx_json_extract('src', ['falg_kw'], type='varchar', alias='falg_kw') }},
        {{ trx_json_extract('src', ['oset'], type='varchar', alias='oset') }},
        {{ trx_json_extract('src', ['cusg'], type='int', alias='cusg') }},
        {{ trx_json_extract('src', ['cusg_kw'], type='varchar', alias='cusg_kw') }},
        {{ trx_json_extract('src', ['vaty'], type='int', alias='vaty') }},
        {{ trx_json_extract('src', ['vaty_kw'], type='varchar', alias='vaty_kw') }},
        {{ trx_json_extract('src', ['kolt'], type='int', alias='kolt') }},
        {{ trx_json_extract('src', ['kolt_kw'], type='varchar', alias='kolt_kw') }},
        {{ trx_json_extract('src', ['norm'], type='float', alias='norm') }},
        {{ trx_json_extract('src', ['chrt'], type='varchar', alias='chrt') }},
        {{ trx_json_extract('src', ['chty'], type='varchar', alias='chty') }},
        {{ trx_json_extract('src', ['tost'], type='varchar', alias='tost') }},
        {{ trx_json_extract('src', ['nomi'], type='float', alias='nomi') }},
        {{ trx_json_extract('src', ['ulmt'], type='float', alias='ulmt') }},
        {{ trx_json_extract('src', ['llmt'], type='float', alias='llmt') }},
        {{ trx_json_extract('src', ['utln'], type='float', alias='utln') }},
        {{ trx_json_extract('src', ['ltln'], type='float', alias='ltln') }},
        {{ trx_json_extract('src', ['utwa'], type='float', alias='utwa') }},
        {{ trx_json_extract('src', ['ltwa'], type='float', alias='ltwa') }},
        {{ trx_json_extract('src', ['utlw'], type='float', alias='utlw') }},
        {{ trx_json_extract('src', ['ltlw'], type='float', alias='ltlw') }},
        {{ trx_json_extract('src', ['imag'], type='varchar', alias='imag') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['algo_ref_compnr'], type='int', alias='algo_ref_compnr') }},
        {{ trx_json_extract('src', ['dtst_ref_compnr'], type='int', alias='dtst_ref_compnr') }},
        {{ trx_json_extract('src', ['chun_ref_compnr'], type='int', alias='chun_ref_compnr') }},
        {{ trx_json_extract('src', ['oset_ref_compnr'], type='int', alias='oset_ref_compnr') }},
        {{ trx_json_extract('src', ['chrt_ref_compnr'], type='int', alias='chrt_ref_compnr') }},
        {{ trx_json_extract('src', ['chty_ref_compnr'], type='int', alias='chty_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_qmptc001') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'char']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
