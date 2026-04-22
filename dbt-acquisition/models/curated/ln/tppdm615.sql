{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppdm615 Project Labor table, Generated 2026-04-10T19:42:00Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['task'], type='varchar', alias='task') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['dfts'], type='varchar', alias='dfts') }},
        {{ trx_json_extract('src', ['ctrg'], type='varchar', alias='ctrg') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['norm'], type='float', alias='norm') }},
        {{ trx_json_extract('src', ['ccts'], type='varchar', alias='ccts') }},
        {{ trx_json_extract('src', ['ccfu'], type='int', alias='ccfu') }},
        {{ trx_json_extract('src', ['ccfu_kw'], type='varchar', alias='ccfu_kw') }},
        {{ trx_json_extract('src', ['ccco'], type='varchar', alias='ccco') }},
        {{ trx_json_extract('src', ['ratc'], type='float', alias='ratc') }},
        {{ trx_json_extract('src', ['cccr'], type='varchar', alias='cccr') }},
        {{ trx_json_extract('src', ['ltrc'], type='timestamp_ntz', alias='ltrc') }},
        {{ trx_json_extract('src', ['dfrc'], type='int', alias='dfrc') }},
        {{ trx_json_extract('src', ['dfrc_kw'], type='varchar', alias='dfrc_kw') }},
        {{ trx_json_extract('src', ['rats'], type='float', alias='rats') }},
        {{ trx_json_extract('src', ['ccsr'], type='varchar', alias='ccsr') }},
        {{ trx_json_extract('src', ['ltrs'], type='timestamp_ntz', alias='ltrs') }},
        {{ trx_json_extract('src', ['dfrs'], type='int', alias='dfrs') }},
        {{ trx_json_extract('src', ['dfrs_kw'], type='varchar', alias='dfrs_kw') }},
        {{ trx_json_extract('src', ['rati'], type='float', alias='rati') }},
        {{ trx_json_extract('src', ['ccir'], type='varchar', alias='ccir') }},
        {{ trx_json_extract('src', ['ltri'], type='timestamp_ntz', alias='ltri') }},
        {{ trx_json_extract('src', ['dfri'], type='int', alias='dfri') }},
        {{ trx_json_extract('src', ['dfri_kw'], type='varchar', alias='dfri_kw') }},
        {{ trx_json_extract('src', ['prre'], type='int', alias='prre') }},
        {{ trx_json_extract('src', ['prre_kw'], type='varchar', alias='prre_kw') }},
        {{ trx_json_extract('src', ['rcmc_1'], type='float', alias='rcmc_1') }},
        {{ trx_json_extract('src', ['rcmc_2'], type='float', alias='rcmc_2') }},
        {{ trx_json_extract('src', ['rcmc_3'], type='float', alias='rcmc_3') }},
        {{ trx_json_extract('src', ['rsmc_1'], type='float', alias='rsmc_1') }},
        {{ trx_json_extract('src', ['rsmc_2'], type='float', alias='rsmc_2') }},
        {{ trx_json_extract('src', ['rsmc_3'], type='float', alias='rsmc_3') }},
        {{ trx_json_extract('src', ['rimc_1'], type='float', alias='rimc_1') }},
        {{ trx_json_extract('src', ['rimc_2'], type='float', alias='rimc_2') }},
        {{ trx_json_extract('src', ['rimc_3'], type='float', alias='rimc_3') }},
        {{ trx_json_extract('src', ['unrt'], type='varchar', alias='unrt') }},
        {{ trx_json_extract('src', ['rfac'], type='varchar', alias='rfac') }},
        {{ trx_json_extract('src', ['cvat'], type='varchar', alias='cvat') }},
        {{ trx_json_extract('src', ['blbl'], type='int', alias='blbl') }},
        {{ trx_json_extract('src', ['blbl_kw'], type='varchar', alias='blbl_kw') }},
        {{ trx_json_extract('src', ['eplf'], type='float', alias='eplf') }},
        {{ trx_json_extract('src', ['cofc'], type='varchar', alias='cofc') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['dfts_ref_compnr'], type='int', alias='dfts_ref_compnr') }},
        {{ trx_json_extract('src', ['ctrg_ref_compnr'], type='int', alias='ctrg_ref_compnr') }},
        {{ trx_json_extract('src', ['cuni_ref_compnr'], type='int', alias='cuni_ref_compnr') }},
        {{ trx_json_extract('src', ['ccco_ref_compnr'], type='int', alias='ccco_ref_compnr') }},
        {{ trx_json_extract('src', ['cccr_ref_compnr'], type='int', alias='cccr_ref_compnr') }},
        {{ trx_json_extract('src', ['ccsr_ref_compnr'], type='int', alias='ccsr_ref_compnr') }},
        {{ trx_json_extract('src', ['ccir_ref_compnr'], type='int', alias='ccir_ref_compnr') }},
        {{ trx_json_extract('src', ['unrt_ref_compnr'], type='int', alias='unrt_ref_compnr') }},
        {{ trx_json_extract('src', ['cvat_ref_compnr'], type='int', alias='cvat_ref_compnr') }},
        {{ trx_json_extract('src', ['cofc_ref_compnr'], type='int', alias='cofc_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_task'], type='varchar', alias='cprj_task') }},
        {{ trx_json_extract('src', ['cprj_ccts'], type='varchar', alias='cprj_ccts') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppdm615') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'task']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
