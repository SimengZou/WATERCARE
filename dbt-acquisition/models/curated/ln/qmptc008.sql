{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN qmptc008 Instruments table, Generated 2022-06-15T01:21:04Z from package combination ce01055',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['inst'], type='varchar', alias='inst') }},
        {{ trx_json_extract('src', ['inno'], type='varchar', alias='inno') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['intg'], type='varchar', alias='intg') }},
        {{ trx_json_extract('src', ['tare'], type='varchar', alias='tare') }},
        {{ trx_json_extract('src', ['skll'], type='varchar', alias='skll') }},
        {{ trx_json_extract('src', ['lcnt'], type='int', alias='lcnt') }},
        {{ trx_json_extract('src', ['culc'], type='varchar', alias='culc') }},
        {{ trx_json_extract('src', ['cbit'], type='int', alias='cbit') }},
        {{ trx_json_extract('src', ['cbit_kw'], type='varchar', alias='cbit_kw') }},
        {{ trx_json_extract('src', ['cbin'], type='int', alias='cbin') }},
        {{ trx_json_extract('src', ['lcdt'], type='timestamp_ntz', alias='lcdt') }},
        {{ trx_json_extract('src', ['ncdt'], type='timestamp_ntz', alias='ncdt') }},
        {{ trx_json_extract('src', ['ncus'], type='int', alias='ncus') }},
        {{ trx_json_extract('src', ['bcal'], type='int', alias='bcal') }},
        {{ trx_json_extract('src', ['bcal_kw'], type='varchar', alias='bcal_kw') }},
        {{ trx_json_extract('src', ['ausg'], type='int', alias='ausg') }},
        {{ trx_json_extract('src', ['cbdt'], type='timestamp_ntz', alias='cbdt') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['txtb'], type='int', alias='txtb') }},
        {{ trx_json_extract('src', ['intg_ref_compnr'], type='int', alias='intg_ref_compnr') }},
        {{ trx_json_extract('src', ['tare_ref_compnr'], type='int', alias='tare_ref_compnr') }},
        {{ trx_json_extract('src', ['skll_ref_compnr'], type='int', alias='skll_ref_compnr') }},
        {{ trx_json_extract('src', ['culc_ref_compnr'], type='int', alias='culc_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['txtb_ref_compnr'], type='int', alias='txtb_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_qmptc008') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'inst', 'inno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
