{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppdm640 Project Sundry Cost Codes table, Generated 2026-04-10T19:42:01Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cico'], type='varchar', alias='cico') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['dfic'], type='varchar', alias='dfic') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['pric'], type='float', alias='pric') }},
        {{ trx_json_extract('src', ['cccp'], type='varchar', alias='cccp') }},
        {{ trx_json_extract('src', ['ltpr'], type='timestamp_ntz', alias='ltpr') }},
        {{ trx_json_extract('src', ['dpri'], type='int', alias='dpri') }},
        {{ trx_json_extract('src', ['dpri_kw'], type='varchar', alias='dpri_kw') }},
        {{ trx_json_extract('src', ['pris'], type='float', alias='pris') }},
        {{ trx_json_extract('src', ['ccsp'], type='varchar', alias='ccsp') }},
        {{ trx_json_extract('src', ['ltsp'], type='timestamp_ntz', alias='ltsp') }},
        {{ trx_json_extract('src', ['dspr'], type='int', alias='dspr') }},
        {{ trx_json_extract('src', ['dspr_kw'], type='varchar', alias='dspr_kw') }},
        {{ trx_json_extract('src', ['prii'], type='float', alias='prii') }},
        {{ trx_json_extract('src', ['ccip'], type='varchar', alias='ccip') }},
        {{ trx_json_extract('src', ['ltip'], type='timestamp_ntz', alias='ltip') }},
        {{ trx_json_extract('src', ['dipr'], type='int', alias='dipr') }},
        {{ trx_json_extract('src', ['dipr_kw'], type='varchar', alias='dipr_kw') }},
        {{ trx_json_extract('src', ['ccco'], type='varchar', alias='ccco') }},
        {{ trx_json_extract('src', ['ccic'], type='varchar', alias='ccic') }},
        {{ trx_json_extract('src', ['ccfu'], type='int', alias='ccfu') }},
        {{ trx_json_extract('src', ['ccfu_kw'], type='varchar', alias='ccfu_kw') }},
        {{ trx_json_extract('src', ['prre'], type='int', alias='prre') }},
        {{ trx_json_extract('src', ['prre_kw'], type='varchar', alias='prre_kw') }},
        {{ trx_json_extract('src', ['pcmc_1'], type='float', alias='pcmc_1') }},
        {{ trx_json_extract('src', ['pcmc_2'], type='float', alias='pcmc_2') }},
        {{ trx_json_extract('src', ['pcmc_3'], type='float', alias='pcmc_3') }},
        {{ trx_json_extract('src', ['psmc_1'], type='float', alias='psmc_1') }},
        {{ trx_json_extract('src', ['psmc_2'], type='float', alias='psmc_2') }},
        {{ trx_json_extract('src', ['psmc_3'], type='float', alias='psmc_3') }},
        {{ trx_json_extract('src', ['pimc_1'], type='float', alias='pimc_1') }},
        {{ trx_json_extract('src', ['pimc_2'], type='float', alias='pimc_2') }},
        {{ trx_json_extract('src', ['pimc_3'], type='float', alias='pimc_3') }},
        {{ trx_json_extract('src', ['blbl'], type='int', alias='blbl') }},
        {{ trx_json_extract('src', ['blbl_kw'], type='varchar', alias='blbl_kw') }},
        {{ trx_json_extract('src', ['cvat'], type='varchar', alias='cvat') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['dfic_ref_compnr'], type='int', alias='dfic_ref_compnr') }},
        {{ trx_json_extract('src', ['cuni_ref_compnr'], type='int', alias='cuni_ref_compnr') }},
        {{ trx_json_extract('src', ['cccp_ref_compnr'], type='int', alias='cccp_ref_compnr') }},
        {{ trx_json_extract('src', ['ccsp_ref_compnr'], type='int', alias='ccsp_ref_compnr') }},
        {{ trx_json_extract('src', ['ccip_ref_compnr'], type='int', alias='ccip_ref_compnr') }},
        {{ trx_json_extract('src', ['ccco_ref_compnr'], type='int', alias='ccco_ref_compnr') }},
        {{ trx_json_extract('src', ['cvat_ref_compnr'], type='int', alias='cvat_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cico'], type='varchar', alias='cprj_cico') }},
        {{ trx_json_extract('src', ['cprj_ccic'], type='varchar', alias='cprj_ccic') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppdm640') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'cico']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
