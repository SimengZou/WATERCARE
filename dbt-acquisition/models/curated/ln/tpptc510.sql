{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpptc510 Budget Amounts of Activities by Version table, Generated 2026-04-10T19:42:27Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['vers'], type='int', alias='vers') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['rgdt'], type='timestamp_ntz', alias='rgdt') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['ambu'], type='float', alias='ambu') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['amit'], type='float', alias='amit') }},
        {{ trx_json_extract('src', ['amts'], type='float', alias='amts') }},
        {{ trx_json_extract('src', ['ameq'], type='float', alias='ameq') }},
        {{ trx_json_extract('src', ['amsb'], type='float', alias='amsb') }},
        {{ trx_json_extract('src', ['amsn'], type='float', alias='amsn') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['rate_1'], type='float', alias='rate_1') }},
        {{ trx_json_extract('src', ['rate_2'], type='float', alias='rate_2') }},
        {{ trx_json_extract('src', ['rate_3'], type='float', alias='rate_3') }},
        {{ trx_json_extract('src', ['rdat'], type='timestamp_ntz', alias='rdat') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_vers_ref_compnr'], type='int', alias='cprj_vers_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpptc510') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'vers', 'cact', 'rgdt']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
