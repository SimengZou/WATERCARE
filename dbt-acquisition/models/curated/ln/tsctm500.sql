{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm500 Generic Warranties table, Generated 2026-04-10T19:42:35Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['gwte'], type='varchar', alias='gwte') }},
        {{ trx_json_extract('src', ['cwte'], type='varchar', alias='cwte') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['uprd'], type='int', alias='uprd') }},
        {{ trx_json_extract('src', ['uprd_kw'], type='varchar', alias='uprd_kw') }},
        {{ trx_json_extract('src', ['nrpe'], type='int', alias='nrpe') }},
        {{ trx_json_extract('src', ['peru'], type='int', alias='peru') }},
        {{ trx_json_extract('src', ['peru_kw'], type='varchar', alias='peru_kw') }},
        {{ trx_json_extract('src', ['cwoc'], type='varchar', alias='cwoc') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['term'], type='int', alias='term') }},
        {{ trx_json_extract('src', ['efdt'], type='date', alias='efdt') }},
        {{ trx_json_extract('src', ['exdt'], type='date', alias='exdt') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cwte_ref_compnr'], type='int', alias='cwte_ref_compnr') }},
        {{ trx_json_extract('src', ['cwoc_ref_compnr'], type='int', alias='cwoc_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm500') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'gwte']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
