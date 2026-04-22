{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsmdm140 Service Employees table, Generated 2026-04-10T19:42:37Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['csar'], type='varchar', alias='csar') }},
        {{ trx_json_extract('src', ['ccar'], type='varchar', alias='ccar') }},
        {{ trx_json_extract('src', ['mopd'], type='float', alias='mopd') }},
        {{ trx_json_extract('src', ['pgen'], type='int', alias='pgen') }},
        {{ trx_json_extract('src', ['pgen_kw'], type='varchar', alias='pgen_kw') }},
        {{ trx_json_extract('src', ['asmc'], type='varchar', alias='asmc') }},
        {{ trx_json_extract('src', ['nots', 'en_US'], type='varchar', alias='nots__en_us') }},
        {{ trx_json_extract('src', ['lsdt'], type='timestamp_ntz', alias='lsdt') }},
        {{ trx_json_extract('src', ['ucts'], type='int', alias='ucts') }},
        {{ trx_json_extract('src', ['ucts_kw'], type='varchar', alias='ucts_kw') }},
        {{ trx_json_extract('src', ['frmn'], type='varchar', alias='frmn') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['csar_ref_compnr'], type='int', alias='csar_ref_compnr') }},
        {{ trx_json_extract('src', ['ccar_ref_compnr'], type='int', alias='ccar_ref_compnr') }},
        {{ trx_json_extract('src', ['asmc_ref_compnr'], type='int', alias='asmc_ref_compnr') }},
        {{ trx_json_extract('src', ['frmn_ref_compnr'], type='int', alias='frmn_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsmdm140') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'emno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
