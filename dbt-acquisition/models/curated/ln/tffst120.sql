{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffst120 Statement Accounts table, Generated 2026-04-10T19:41:38Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['fstm'], type='varchar', alias='fstm') }},
        {{ trx_json_extract('src', ['accn'], type='varchar', alias='accn') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['subl'], type='int', alias='subl') }},
        {{ trx_json_extract('src', ['pacc'], type='varchar', alias='pacc') }},
        {{ trx_json_extract('src', ['dcsw'], type='int', alias='dcsw') }},
        {{ trx_json_extract('src', ['dcsw_kw'], type='varchar', alias='dcsw_kw') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['rhis'], type='int', alias='rhis') }},
        {{ trx_json_extract('src', ['rhis_kw'], type='varchar', alias='rhis_kw') }},
        {{ trx_json_extract('src', ['accs'], type='int', alias='accs') }},
        {{ trx_json_extract('src', ['accs_kw'], type='varchar', alias='accs_kw') }},
        {{ trx_json_extract('src', ['acct'], type='int', alias='acct') }},
        {{ trx_json_extract('src', ['acct_kw'], type='varchar', alias='acct_kw') }},
        {{ trx_json_extract('src', ['cfsa'], type='int', alias='cfsa') }},
        {{ trx_json_extract('src', ['cfsa_kw'], type='varchar', alias='cfsa_kw') }},
        {{ trx_json_extract('src', ['dbcr'], type='int', alias='dbcr') }},
        {{ trx_json_extract('src', ['dbcr_kw'], type='varchar', alias='dbcr_kw') }},
        {{ trx_json_extract('src', ['dcgl'], type='int', alias='dcgl') }},
        {{ trx_json_extract('src', ['dcgl_kw'], type='varchar', alias='dcgl_kw') }},
        {{ trx_json_extract('src', ['sign'], type='int', alias='sign') }},
        {{ trx_json_extract('src', ['sign_kw'], type='varchar', alias='sign_kw') }},
        {{ trx_json_extract('src', ['acca'], type='varchar', alias='acca') }},
        {{ trx_json_extract('src', ['muaa'], type='int', alias='muaa') }},
        {{ trx_json_extract('src', ['muaa_kw'], type='varchar', alias='muaa_kw') }},
        {{ trx_json_extract('src', ['acrd'], type='varchar', alias='acrd') }},
        {{ trx_json_extract('src', ['cgla'], type='varchar', alias='cgla') }},
        {{ trx_json_extract('src', ['facc'], type='varchar', alias='facc') }},
        {{ trx_json_extract('src', ['prta'], type='int', alias='prta') }},
        {{ trx_json_extract('src', ['prta_kw'], type='varchar', alias='prta_kw') }},
        {{ trx_json_extract('src', ['pseq'], type='int', alias='pseq') }},
        {{ trx_json_extract('src', ['atxt'], type='int', alias='atxt') }},
        {{ trx_json_extract('src', ['fstm_acca_ref_compnr'], type='int', alias='fstm_acca_ref_compnr') }},
        {{ trx_json_extract('src', ['fstm_facc_ref_compnr'], type='int', alias='fstm_facc_ref_compnr') }},
        {{ trx_json_extract('src', ['fstm_acrd_ref_compnr'], type='int', alias='fstm_acrd_ref_compnr') }},
        {{ trx_json_extract('src', ['fstm_pacc_ref_compnr'], type='int', alias='fstm_pacc_ref_compnr') }},
        {{ trx_json_extract('src', ['fstm_cgla_ref_compnr'], type='int', alias='fstm_cgla_ref_compnr') }},
        {{ trx_json_extract('src', ['fstm_ref_compnr'], type='int', alias='fstm_ref_compnr') }},
        {{ trx_json_extract('src', ['rtyp_ref_compnr'], type='int', alias='rtyp_ref_compnr') }},
        {{ trx_json_extract('src', ['atxt_ref_compnr'], type='int', alias='atxt_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffst120') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'fstm', 'accn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
