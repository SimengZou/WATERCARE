{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpptc316 Actual Budget by Activity/Cost Component table, Generated 2026-04-10T19:42:26Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['ccco'], type='varchar', alias='ccco') }},
        {{ trx_json_extract('src', ['amac_1'], type='float', alias='amac_1') }},
        {{ trx_json_extract('src', ['amac_2'], type='float', alias='amac_2') }},
        {{ trx_json_extract('src', ['amac_3'], type='float', alias='amac_3') }},
        {{ trx_json_extract('src', ['amac_4'], type='float', alias='amac_4') }},
        {{ trx_json_extract('src', ['atac_1'], type='float', alias='atac_1') }},
        {{ trx_json_extract('src', ['atac_2'], type='float', alias='atac_2') }},
        {{ trx_json_extract('src', ['atac_3'], type='float', alias='atac_3') }},
        {{ trx_json_extract('src', ['atac_4'], type='float', alias='atac_4') }},
        {{ trx_json_extract('src', ['qtah'], type='float', alias='qtah') }},
        {{ trx_json_extract('src', ['aeqc_1'], type='float', alias='aeqc_1') }},
        {{ trx_json_extract('src', ['aeqc_2'], type='float', alias='aeqc_2') }},
        {{ trx_json_extract('src', ['aeqc_3'], type='float', alias='aeqc_3') }},
        {{ trx_json_extract('src', ['aeqc_4'], type='float', alias='aeqc_4') }},
        {{ trx_json_extract('src', ['asbc_1'], type='float', alias='asbc_1') }},
        {{ trx_json_extract('src', ['asbc_2'], type='float', alias='asbc_2') }},
        {{ trx_json_extract('src', ['asbc_3'], type='float', alias='asbc_3') }},
        {{ trx_json_extract('src', ['asbc_4'], type='float', alias='asbc_4') }},
        {{ trx_json_extract('src', ['aicl_1'], type='float', alias='aicl_1') }},
        {{ trx_json_extract('src', ['aicl_2'], type='float', alias='aicl_2') }},
        {{ trx_json_extract('src', ['aicl_3'], type='float', alias='aicl_3') }},
        {{ trx_json_extract('src', ['aicl_4'], type='float', alias='aicl_4') }},
        {{ trx_json_extract('src', ['aicc_1'], type='float', alias='aicc_1') }},
        {{ trx_json_extract('src', ['aicc_2'], type='float', alias='aicc_2') }},
        {{ trx_json_extract('src', ['aicc_3'], type='float', alias='aicc_3') }},
        {{ trx_json_extract('src', ['aicc_4'], type='float', alias='aicc_4') }},
        {{ trx_json_extract('src', ['cprj_cpla_ref_compnr'], type='int', alias='cprj_cpla_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ccal_ref_compnr'], type='int', alias='cprj_ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ccco_ref_compnr'], type='int', alias='ccco_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpptc316') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'ccal', 'cpla', 'cact', 'ccco']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
