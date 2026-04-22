{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN qmcpl100 Corrective Action Plan table, Generated 2022-06-15T01:21:00Z from package combination ce01055',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['capn'], type='varchar', alias='capn') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['ownr'], type='varchar', alias='ownr') }},
        {{ trx_json_extract('src', ['appr'], type='varchar', alias='appr') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['effn'], type='int', alias='effn') }},
        {{ trx_json_extract('src', ['revi'], type='varchar', alias='revi') }},
        {{ trx_json_extract('src', ['dudt'], type='timestamp_ntz', alias='dudt') }},
        {{ trx_json_extract('src', ['cmdt'], type='timestamp_ntz', alias='cmdt') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['cusr'], type='varchar', alias='cusr') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['catg'], type='varchar', alias='catg') }},
        {{ trx_json_extract('src', ['apdt'], type='timestamp_ntz', alias='apdt') }},
        {{ trx_json_extract('src', ['sudt'], type='timestamp_ntz', alias='sudt') }},
        {{ trx_json_extract('src', ['modt'], type='timestamp_ntz', alias='modt') }},
        {{ trx_json_extract('src', ['cndt'], type='timestamp_ntz', alias='cndt') }},
        {{ trx_json_extract('src', ['cpef'], type='int', alias='cpef') }},
        {{ trx_json_extract('src', ['cpef_kw'], type='varchar', alias='cpef_kw') }},
        {{ trx_json_extract('src', ['ecpv'], type='int', alias='ecpv') }},
        {{ trx_json_extract('src', ['ecpv_kw'], type='varchar', alias='ecpv_kw') }},
        {{ trx_json_extract('src', ['mrbc'], type='varchar', alias='mrbc') }},
        {{ trx_json_extract('src', ['pbsm'], type='int', alias='pbsm') }},
        {{ trx_json_extract('src', ['pbsm_kw'], type='varchar', alias='pbsm_kw') }},
        {{ trx_json_extract('src', ['psmo', 'en_US'], type='varchar', alias='psmo__en_us') }},
        {{ trx_json_extract('src', ['rcad'], type='varchar', alias='rcad') }},
        {{ trx_json_extract('src', ['scap'], type='int', alias='scap') }},
        {{ trx_json_extract('src', ['scap_kw'], type='varchar', alias='scap_kw') }},
        {{ trx_json_extract('src', ['scas'], type='int', alias='scas') }},
        {{ trx_json_extract('src', ['scas_kw'], type='varchar', alias='scas_kw') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['efdt'], type='timestamp_ntz', alias='efdt') }},
        {{ trx_json_extract('src', ['codt'], type='timestamp_ntz', alias='codt') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['pbst'], type='int', alias='pbst') }},
        {{ trx_json_extract('src', ['comt'], type='int', alias='comt') }},
        {{ trx_json_extract('src', ['bgif'], type='int', alias='bgif') }},
        {{ trx_json_extract('src', ['crst'], type='int', alias='crst') }},
        {{ trx_json_extract('src', ['goal'], type='int', alias='goal') }},
        {{ trx_json_extract('src', ['cfef'], type='int', alias='cfef') }},
        {{ trx_json_extract('src', ['evds'], type='int', alias='evds') }},
        {{ trx_json_extract('src', ['imag'], type='varchar', alias='imag') }},
        {{ trx_json_extract('src', ['ownr_ref_compnr'], type='int', alias='ownr_ref_compnr') }},
        {{ trx_json_extract('src', ['appr_ref_compnr'], type='int', alias='appr_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['effn_ref_compnr'], type='int', alias='effn_ref_compnr') }},
        {{ trx_json_extract('src', ['bpid_ref_compnr'], type='int', alias='bpid_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['catg_ref_compnr'], type='int', alias='catg_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['pbst_ref_compnr'], type='int', alias='pbst_ref_compnr') }},
        {{ trx_json_extract('src', ['comt_ref_compnr'], type='int', alias='comt_ref_compnr') }},
        {{ trx_json_extract('src', ['bgif_ref_compnr'], type='int', alias='bgif_ref_compnr') }},
        {{ trx_json_extract('src', ['crst_ref_compnr'], type='int', alias='crst_ref_compnr') }},
        {{ trx_json_extract('src', ['goal_ref_compnr'], type='int', alias='goal_ref_compnr') }},
        {{ trx_json_extract('src', ['cfef_ref_compnr'], type='int', alias='cfef_ref_compnr') }},
        {{ trx_json_extract('src', ['evds_ref_compnr'], type='int', alias='evds_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_qmcpl100') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'capn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
