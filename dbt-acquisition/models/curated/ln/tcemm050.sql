{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcemm050 Sites table, Generated 2026-04-10T19:41:06Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['lcmp'], type='int', alias='lcmp') }},
        {{ trx_json_extract('src', ['cadr'], type='varchar', alias='cadr') }},
        {{ trx_json_extract('src', ['admo'], type='int', alias='admo') }},
        {{ trx_json_extract('src', ['admo_kw'], type='varchar', alias='admo_kw') }},
        {{ trx_json_extract('src', ['clus'], type='varchar', alias='clus') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['ract'], type='varchar', alias='ract') }},
        {{ trx_json_extract('src', ['eunt'], type='varchar', alias='eunt') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['xsit'], type='int', alias='xsit') }},
        {{ trx_json_extract('src', ['xsit_kw'], type='varchar', alias='xsit_kw') }},
        {{ trx_json_extract('src', ['subs'], type='int', alias='subs') }},
        {{ trx_json_extract('src', ['subs_kw'], type='varchar', alias='subs_kw') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['sfbp'], type='varchar', alias='sfbp') }},
        {{ trx_json_extract('src', ['cust'], type='int', alias='cust') }},
        {{ trx_json_extract('src', ['cust_kw'], type='varchar', alias='cust_kw') }},
        {{ trx_json_extract('src', ['ofbp'], type='varchar', alias='ofbp') }},
        {{ trx_json_extract('src', ['stbp'], type='varchar', alias='stbp') }},
        {{ trx_json_extract('src', ['expl'], type='int', alias='expl') }},
        {{ trx_json_extract('src', ['expl_kw'], type='varchar', alias='expl_kw') }},
        {{ trx_json_extract('src', ['scat'], type='varchar', alias='scat') }},
        {{ trx_json_extract('src', ['psit'], type='varchar', alias='psit') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['crby'], type='varchar', alias='crby') }},
        {{ trx_json_extract('src', ['inf1', 'en_US'], type='varchar', alias='inf1__en_us') }},
        {{ trx_json_extract('src', ['inf2', 'en_US'], type='varchar', alias='inf2__en_us') }},
        {{ trx_json_extract('src', ['imag'], type='varchar', alias='imag') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['lcmp_ref_compnr'], type='int', alias='lcmp_ref_compnr') }},
        {{ trx_json_extract('src', ['cadr_ref_compnr'], type='int', alias='cadr_ref_compnr') }},
        {{ trx_json_extract('src', ['clus_ref_compnr'], type='int', alias='clus_ref_compnr') }},
        {{ trx_json_extract('src', ['ccal_ref_compnr'], type='int', alias='ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['ract_ref_compnr'], type='int', alias='ract_ref_compnr') }},
        {{ trx_json_extract('src', ['eunt_ref_compnr'], type='int', alias='eunt_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['bpid_ref_compnr'], type='int', alias='bpid_ref_compnr') }},
        {{ trx_json_extract('src', ['sfbp_ref_compnr'], type='int', alias='sfbp_ref_compnr') }},
        {{ trx_json_extract('src', ['ofbp_ref_compnr'], type='int', alias='ofbp_ref_compnr') }},
        {{ trx_json_extract('src', ['stbp_ref_compnr'], type='int', alias='stbp_ref_compnr') }},
        {{ trx_json_extract('src', ['scat_ref_compnr'], type='int', alias='scat_ref_compnr') }},
        {{ trx_json_extract('src', ['psit_ref_compnr'], type='int', alias='psit_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcemm050') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'site']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
