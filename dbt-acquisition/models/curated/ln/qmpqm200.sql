{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN qmpqm200 Failure Report Analysis and Corrective Action System table, Generated 2022-06-15T01:21:03Z from package combination ce01055',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['frac'], type='varchar', alias='frac') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['ftyp'], type='int', alias='ftyp') }},
        {{ trx_json_extract('src', ['ftyp_kw'], type='varchar', alias='ftyp_kw') }},
        {{ trx_json_extract('src', ['othr', 'en_US'], type='varchar', alias='othr__en_us') }},
        {{ trx_json_extract('src', ['orgn'], type='int', alias='orgn') }},
        {{ trx_json_extract('src', ['orgn_kw'], type='varchar', alias='orgn_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['opno'], type='int', alias='opno') }},
        {{ trx_json_extract('src', ['styp'], type='int', alias='styp') }},
        {{ trx_json_extract('src', ['styp_kw'], type='varchar', alias='styp_kw') }},
        {{ trx_json_extract('src', ['rpas'], type='int', alias='rpas') }},
        {{ trx_json_extract('src', ['rpas_kw'], type='varchar', alias='rpas_kw') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['revi'], type='varchar', alias='revi') }},
        {{ trx_json_extract('src', ['effn'], type='int', alias='effn') }},
        {{ trx_json_extract('src', ['cser'], type='varchar', alias='cser') }},
        {{ trx_json_extract('src', ['clot'], type='varchar', alias='clot') }},
        {{ trx_json_extract('src', ['pitm'], type='varchar', alias='pitm') }},
        {{ trx_json_extract('src', ['pser'], type='varchar', alias='pser') }},
        {{ trx_json_extract('src', ['plot'], type='varchar', alias='plot') }},
        {{ trx_json_extract('src', ['lfcr'], type='varchar', alias='lfcr') }},
        {{ trx_json_extract('src', ['lfln'], type='int', alias='lfln') }},
        {{ trx_json_extract('src', ['frst'], type='varchar', alias='frst') }},
        {{ trx_json_extract('src', ['frcd'], type='varchar', alias='frcd') }},
        {{ trx_json_extract('src', ['rptd'], type='int', alias='rptd') }},
        {{ trx_json_extract('src', ['rptd_kw'], type='varchar', alias='rptd_kw') }},
        {{ trx_json_extract('src', ['frct'], type='varchar', alias='frct') }},
        {{ trx_json_extract('src', ['intg'], type='varchar', alias='intg') }},
        {{ trx_json_extract('src', ['inst'], type='varchar', alias='inst') }},
        {{ trx_json_extract('src', ['inno'], type='varchar', alias='inno') }},
        {{ trx_json_extract('src', ['frpt'], type='varchar', alias='frpt') }},
        {{ trx_json_extract('src', ['fdat'], type='timestamp_ntz', alias='fdat') }},
        {{ trx_json_extract('src', ['fcrt'], type='varchar', alias='fcrt') }},
        {{ trx_json_extract('src', ['fsld'], type='timestamp_ntz', alias='fsld') }},
        {{ trx_json_extract('src', ['cnrs'], type='varchar', alias='cnrs') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['mrbc'], type='varchar', alias='mrbc') }},
        {{ trx_json_extract('src', ['ftxt'], type='int', alias='ftxt') }},
        {{ trx_json_extract('src', ['bpid_ref_compnr'], type='int', alias='bpid_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['pitm_ref_compnr'], type='int', alias='pitm_ref_compnr') }},
        {{ trx_json_extract('src', ['frst_ref_compnr'], type='int', alias='frst_ref_compnr') }},
        {{ trx_json_extract('src', ['frcd_ref_compnr'], type='int', alias='frcd_ref_compnr') }},
        {{ trx_json_extract('src', ['frct_ref_compnr'], type='int', alias='frct_ref_compnr') }},
        {{ trx_json_extract('src', ['intg_ref_compnr'], type='int', alias='intg_ref_compnr') }},
        {{ trx_json_extract('src', ['cnrs_ref_compnr'], type='int', alias='cnrs_ref_compnr') }},
        {{ trx_json_extract('src', ['ftxt_ref_compnr'], type='int', alias='ftxt_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_qmpqm200') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'frac']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
