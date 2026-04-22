{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpppc454 Cost Control by Project / Activity / Sundry Cost Control Code table, Generated 2026-04-10T19:42:16Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cpla'], type='varchar', alias='cpla') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['ccic'], type='varchar', alias='ccic') }},
        {{ trx_json_extract('src', ['ambg_1'], type='float', alias='ambg_1') }},
        {{ trx_json_extract('src', ['ambg_2'], type='float', alias='ambg_2') }},
        {{ trx_json_extract('src', ['ambg_3'], type='float', alias='ambg_3') }},
        {{ trx_json_extract('src', ['ambg_4'], type='float', alias='ambg_4') }},
        {{ trx_json_extract('src', ['qubg'], type='float', alias='qubg') }},
        {{ trx_json_extract('src', ['ampm_1'], type='float', alias='ampm_1') }},
        {{ trx_json_extract('src', ['ampm_2'], type='float', alias='ampm_2') }},
        {{ trx_json_extract('src', ['ampm_3'], type='float', alias='ampm_3') }},
        {{ trx_json_extract('src', ['ampm_4'], type='float', alias='ampm_4') }},
        {{ trx_json_extract('src', ['qupm'], type='float', alias='qupm') }},
        {{ trx_json_extract('src', ['amex_1'], type='float', alias='amex_1') }},
        {{ trx_json_extract('src', ['amex_2'], type='float', alias='amex_2') }},
        {{ trx_json_extract('src', ['amex_3'], type='float', alias='amex_3') }},
        {{ trx_json_extract('src', ['amex_4'], type='float', alias='amex_4') }},
        {{ trx_json_extract('src', ['quex'], type='float', alias='quex') }},
        {{ trx_json_extract('src', ['amac_1'], type='float', alias='amac_1') }},
        {{ trx_json_extract('src', ['amac_2'], type='float', alias='amac_2') }},
        {{ trx_json_extract('src', ['amac_3'], type='float', alias='amac_3') }},
        {{ trx_json_extract('src', ['amac_4'], type='float', alias='amac_4') }},
        {{ trx_json_extract('src', ['quac'], type='float', alias='quac') }},
        {{ trx_json_extract('src', ['ampe_1'], type='float', alias='ampe_1') }},
        {{ trx_json_extract('src', ['ampe_2'], type='float', alias='ampe_2') }},
        {{ trx_json_extract('src', ['ampe_3'], type='float', alias='ampe_3') }},
        {{ trx_json_extract('src', ['ampe_4'], type='float', alias='ampe_4') }},
        {{ trx_json_extract('src', ['qupe'], type='float', alias='qupe') }},
        {{ trx_json_extract('src', ['ampp_1'], type='float', alias='ampp_1') }},
        {{ trx_json_extract('src', ['ampp_2'], type='float', alias='ampp_2') }},
        {{ trx_json_extract('src', ['ampp_3'], type='float', alias='ampp_3') }},
        {{ trx_json_extract('src', ['ampp_4'], type='float', alias='ampp_4') }},
        {{ trx_json_extract('src', ['qupp'], type='float', alias='qupp') }},
        {{ trx_json_extract('src', ['amep_1'], type='float', alias='amep_1') }},
        {{ trx_json_extract('src', ['amep_2'], type='float', alias='amep_2') }},
        {{ trx_json_extract('src', ['amep_3'], type='float', alias='amep_3') }},
        {{ trx_json_extract('src', ['amep_4'], type='float', alias='amep_4') }},
        {{ trx_json_extract('src', ['quep'], type='float', alias='quep') }},
        {{ trx_json_extract('src', ['amap_1'], type='float', alias='amap_1') }},
        {{ trx_json_extract('src', ['amap_2'], type='float', alias='amap_2') }},
        {{ trx_json_extract('src', ['amap_3'], type='float', alias='amap_3') }},
        {{ trx_json_extract('src', ['amap_4'], type='float', alias='amap_4') }},
        {{ trx_json_extract('src', ['quap'], type='float', alias='quap') }},
        {{ trx_json_extract('src', ['cprj_cpla_cact_ref_compnr'], type='int', alias='cprj_cpla_cact_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpppc454') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'cpla', 'cact', 'ccic']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
