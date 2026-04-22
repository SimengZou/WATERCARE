{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffst100 Financial Statements table, Generated 2026-04-10T19:41:38Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['fstm'], type='varchar', alias='fstm') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['ouer'], type='int', alias='ouer') }},
        {{ trx_json_extract('src', ['ouer_kw'], type='varchar', alias='ouer_kw') }},
        {{ trx_json_extract('src', ['styp'], type='int', alias='styp') }},
        {{ trx_json_extract('src', ['styp_kw'], type='varchar', alias='styp_kw') }},
        {{ trx_json_extract('src', ['usrc'], type='varchar', alias='usrc') }},
        {{ trx_json_extract('src', ['datc'], type='timestamp_ntz', alias='datc') }},
        {{ trx_json_extract('src', ['usrm'], type='varchar', alias='usrm') }},
        {{ trx_json_extract('src', ['datm'], type='timestamp_ntz', alias='datm') }},
        {{ trx_json_extract('src', ['mdfs'], type='timestamp_ntz', alias='mdfs') }},
        {{ trx_json_extract('src', ['rcur'], type='varchar', alias='rcur') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['cugl'], type='int', alias='cugl') }},
        {{ trx_json_extract('src', ['cugl_kw'], type='varchar', alias='cugl_kw') }},
        {{ trx_json_extract('src', ['rhis'], type='int', alias='rhis') }},
        {{ trx_json_extract('src', ['rhis_kw'], type='varchar', alias='rhis_kw') }},
        {{ trx_json_extract('src', ['accs'], type='int', alias='accs') }},
        {{ trx_json_extract('src', ['accs_kw'], type='varchar', alias='accs_kw') }},
        {{ trx_json_extract('src', ['nstm'], type='varchar', alias='nstm') }},
        {{ trx_json_extract('src', ['stlt'], type='varchar', alias='stlt') }},
        {{ trx_json_extract('src', ['sltp'], type='int', alias='sltp') }},
        {{ trx_json_extract('src', ['sltp_kw'], type='varchar', alias='sltp_kw') }},
        {{ trx_json_extract('src', ['pann'], type='int', alias='pann') }},
        {{ trx_json_extract('src', ['pann_kw'], type='varchar', alias='pann_kw') }},
        {{ trx_json_extract('src', ['salt'], type='varchar', alias='salt') }},
        {{ trx_json_extract('src', ['altp'], type='int', alias='altp') }},
        {{ trx_json_extract('src', ['altp_kw'], type='varchar', alias='altp_kw') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['cagr'], type='varchar', alias='cagr') }},
        {{ trx_json_extract('src', ['ptra'], type='int', alias='ptra') }},
        {{ trx_json_extract('src', ['ptra_kw'], type='varchar', alias='ptra_kw') }},
        {{ trx_json_extract('src', ['sthe'], type='int', alias='sthe') }},
        {{ trx_json_extract('src', ['anhe'], type='int', alias='anhe') }},
        {{ trx_json_extract('src', ['taxo'], type='varchar', alias='taxo') }},
        {{ trx_json_extract('src', ['vers'], type='int', alias='vers') }},
        {{ trx_json_extract('src', ['txdt'], type='timestamp_ntz', alias='txdt') }},
        {{ trx_json_extract('src', ['rcur_ref_compnr'], type='int', alias='rcur_ref_compnr') }},
        {{ trx_json_extract('src', ['rtyp_ref_compnr'], type='int', alias='rtyp_ref_compnr') }},
        {{ trx_json_extract('src', ['nstm_ref_compnr'], type='int', alias='nstm_ref_compnr') }},
        {{ trx_json_extract('src', ['sltp_stlt_ref_compnr'], type='int', alias='sltp_stlt_ref_compnr') }},
        {{ trx_json_extract('src', ['altp_salt_ref_compnr'], type='int', alias='altp_salt_ref_compnr') }},
        {{ trx_json_extract('src', ['cagr_ref_compnr'], type='int', alias='cagr_ref_compnr') }},
        {{ trx_json_extract('src', ['sthe_ref_compnr'], type='int', alias='sthe_ref_compnr') }},
        {{ trx_json_extract('src', ['anhe_ref_compnr'], type='int', alias='anhe_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffst100') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'fstm']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
