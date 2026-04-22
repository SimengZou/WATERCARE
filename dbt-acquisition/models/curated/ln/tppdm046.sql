{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tppdm046 Third Parties table, Generated 2026-04-10T19:41:56Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cint'], type='varchar', alias='cint') }},
        {{ trx_json_extract('src', ['cins'], type='varchar', alias='cins') }},
        {{ trx_json_extract('src', ['name', 'en_US'], type='varchar', alias='name__en_us') }},
        {{ trx_json_extract('src', ['ccnt'], type='varchar', alias='ccnt') }},
        {{ trx_json_extract('src', ['padr'], type='varchar', alias='padr') }},
        {{ trx_json_extract('src', ['vadr'], type='varchar', alias='vadr') }},
        {{ trx_json_extract('src', ['cint_ref_compnr'], type='int', alias='cint_ref_compnr') }},
        {{ trx_json_extract('src', ['ccnt_ref_compnr'], type='int', alias='ccnt_ref_compnr') }},
        {{ trx_json_extract('src', ['padr_ref_compnr'], type='int', alias='padr_ref_compnr') }},
        {{ trx_json_extract('src', ['vadr_ref_compnr'], type='int', alias='vadr_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tppdm046') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cint', 'cins']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
