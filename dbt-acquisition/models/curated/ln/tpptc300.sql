{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpptc300 Budget Cost Analysis Versions by Project table, Generated 2026-04-10T19:42:25Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['ibud'], type='int', alias='ibud') }},
        {{ trx_json_extract('src', ['ibud_kw'], type='varchar', alias='ibud_kw') }},
        {{ trx_json_extract('src', ['upmd'], type='int', alias='upmd') }},
        {{ trx_json_extract('src', ['upmd_kw'], type='varchar', alias='upmd_kw') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['lcdt'], type='timestamp_ntz', alias='lcdt') }},
        {{ trx_json_extract('src', ['lclg'], type='varchar', alias='lclg') }},
        {{ trx_json_extract('src', ['bdtp'], type='int', alias='bdtp') }},
        {{ trx_json_extract('src', ['bdtp_kw'], type='varchar', alias='bdtp_kw') }},
        {{ trx_json_extract('src', ['free'], type='int', alias='free') }},
        {{ trx_json_extract('src', ['free_kw'], type='varchar', alias='free_kw') }},
        {{ trx_json_extract('src', ['actl'], type='int', alias='actl') }},
        {{ trx_json_extract('src', ['actl_kw'], type='varchar', alias='actl_kw') }},
        {{ trx_json_extract('src', ['defn'], type='int', alias='defn') }},
        {{ trx_json_extract('src', ['defn_kw'], type='varchar', alias='defn_kw') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cexc'], type='varchar', alias='cexc') }},
        {{ trx_json_extract('src', ['exdt'], type='timestamp_ntz', alias='exdt') }},
        {{ trx_json_extract('src', ['iexc'], type='int', alias='iexc') }},
        {{ trx_json_extract('src', ['iexc_kw'], type='varchar', alias='iexc_kw') }},
        {{ trx_json_extract('src', ['icon'], type='int', alias='icon') }},
        {{ trx_json_extract('src', ['icon_kw'], type='varchar', alias='icon_kw') }},
        {{ trx_json_extract('src', ['cprj_cpla_ref_compnr'], type='int', alias='cprj_cpla_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['cexc_ref_compnr'], type='int', alias='cexc_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpptc300') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'ccal']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
