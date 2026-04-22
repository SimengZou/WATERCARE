{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs000 MCS Parameters table, Generated 2026-04-10T19:41:08Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['indt'], type='timestamp_ntz', alias='indt') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['cwei'], type='varchar', alias='cwei') }},
        {{ trx_json_extract('src', ['clen'], type='varchar', alias='clen') }},
        {{ trx_json_extract('src', ['care'], type='varchar', alias='care') }},
        {{ trx_json_extract('src', ['cvol'], type='varchar', alias='cvol') }},
        {{ trx_json_extract('src', ['ctim'], type='varchar', alias='ctim') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['logn'], type='varchar', alias='logn') }},
        {{ trx_json_extract('src', ['cwei_ref_compnr'], type='int', alias='cwei_ref_compnr') }},
        {{ trx_json_extract('src', ['clen_ref_compnr'], type='int', alias='clen_ref_compnr') }},
        {{ trx_json_extract('src', ['care_ref_compnr'], type='int', alias='care_ref_compnr') }},
        {{ trx_json_extract('src', ['cvol_ref_compnr'], type='int', alias='cvol_ref_compnr') }},
        {{ trx_json_extract('src', ['ctim_ref_compnr'], type='int', alias='ctim_ref_compnr') }},
        {{ trx_json_extract('src', ['rtyp_ref_compnr'], type='int', alias='rtyp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs000') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'indt']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
