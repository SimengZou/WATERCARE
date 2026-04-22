{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tpptc354 Actual Budget by Sundry Cost Control Code table, Generated 2026-04-10T19:42:27Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['ccic'], type='varchar', alias='ccic') }},
        {{ trx_json_extract('src', ['ames_1'], type='float', alias='ames_1') }},
        {{ trx_json_extract('src', ['ames_2'], type='float', alias='ames_2') }},
        {{ trx_json_extract('src', ['ames_3'], type='float', alias='ames_3') }},
        {{ trx_json_extract('src', ['ames_4'], type='float', alias='ames_4') }},
        {{ trx_json_extract('src', ['amch_1'], type='float', alias='amch_1') }},
        {{ trx_json_extract('src', ['amch_2'], type='float', alias='amch_2') }},
        {{ trx_json_extract('src', ['amch_3'], type='float', alias='amch_3') }},
        {{ trx_json_extract('src', ['amch_4'], type='float', alias='amch_4') }},
        {{ trx_json_extract('src', ['quan'], type='float', alias='quan') }},
        {{ trx_json_extract('src', ['cprj_ccal_ref_compnr'], type='int', alias='cprj_ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tpptc354') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj', 'ccal', 'ccic']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
