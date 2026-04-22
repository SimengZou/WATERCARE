{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs003 Warehouses table, Generated 2026-04-10T19:41:09Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['cadr'], type='varchar', alias='cadr') }},
        {{ trx_json_extract('src', ['typw'], type='int', alias='typw') }},
        {{ trx_json_extract('src', ['typw_kw'], type='varchar', alias='typw_kw') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['comp'], type='int', alias='comp') }},
        {{ trx_json_extract('src', ['wmsc'], type='int', alias='wmsc') }},
        {{ trx_json_extract('src', ['wmsc_kw'], type='varchar', alias='wmsc_kw') }},
        {{ trx_json_extract('src', ['mesc'], type='int', alias='mesc') }},
        {{ trx_json_extract('src', ['mesc_kw'], type='varchar', alias='mesc_kw') }},
        {{ trx_json_extract('src', ['casi'], type='varchar', alias='casi') }},
        {{ trx_json_extract('src', ['inep'], type='int', alias='inep') }},
        {{ trx_json_extract('src', ['inep_kw'], type='varchar', alias='inep_kw') }},
        {{ trx_json_extract('src', ['imgt'], type='int', alias='imgt') }},
        {{ trx_json_extract('src', ['imgt_kw'], type='varchar', alias='imgt_kw') }},
        {{ trx_json_extract('src', ['imbp'], type='varchar', alias='imbp') }},
        {{ trx_json_extract('src', ['xsit'], type='int', alias='xsit') }},
        {{ trx_json_extract('src', ['xsit_kw'], type='varchar', alias='xsit_kw') }},
        {{ trx_json_extract('src', ['xsbp'], type='varchar', alias='xsbp') }},
        {{ trx_json_extract('src', ['sfbp'], type='varchar', alias='sfbp') }},
        {{ trx_json_extract('src', ['otbp'], type='varchar', alias='otbp') }},
        {{ trx_json_extract('src', ['stbp'], type='varchar', alias='stbp') }},
        {{ trx_json_extract('src', ['ofbp'], type='varchar', alias='ofbp') }},
        {{ trx_json_extract('src', ['clan'], type='varchar', alias='clan') }},
        {{ trx_json_extract('src', ['pwip'], type='int', alias='pwip') }},
        {{ trx_json_extract('src', ['pwip_kw'], type='varchar', alias='pwip_kw') }},
        {{ trx_json_extract('src', ['swhu'], type='int', alias='swhu') }},
        {{ trx_json_extract('src', ['swhu_kw'], type='varchar', alias='swhu_kw') }},
        {{ trx_json_extract('src', ['duns'], type='int', alias='duns') }},
        {{ trx_json_extract('src', ['cdf_updt'], type='timestamp_ntz', alias='cdf_updt') }},
        {{ trx_json_extract('src', ['cadr_ref_compnr'], type='int', alias='cadr_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['casi_ref_compnr'], type='int', alias='casi_ref_compnr') }},
        {{ trx_json_extract('src', ['imbp_ref_compnr'], type='int', alias='imbp_ref_compnr') }},
        {{ trx_json_extract('src', ['xsbp_ref_compnr'], type='int', alias='xsbp_ref_compnr') }},
        {{ trx_json_extract('src', ['sfbp_ref_compnr'], type='int', alias='sfbp_ref_compnr') }},
        {{ trx_json_extract('src', ['otbp_ref_compnr'], type='int', alias='otbp_ref_compnr') }},
        {{ trx_json_extract('src', ['stbp_ref_compnr'], type='int', alias='stbp_ref_compnr') }},
        {{ trx_json_extract('src', ['ofbp_ref_compnr'], type='int', alias='ofbp_ref_compnr') }},
        {{ trx_json_extract('src', ['clan_ref_compnr'], type='int', alias='clan_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_eunt'], type='varchar', alias='cwar_eunt') }},
        {{ trx_json_extract('src', ['eunt_rcmp'], type='int', alias='eunt_rcmp') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs003') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cwar']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
