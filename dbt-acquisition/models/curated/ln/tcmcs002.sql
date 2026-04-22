{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs002 Currencies table, Generated 2026-04-10T19:41:08Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['dscb', 'en_US'], type='varchar', alias='dscb__en_us') }},
        {{ trx_json_extract('src', ['crnd'], type='float', alias='crnd') }},
        {{ trx_json_extract('src', ['emuc'], type='int', alias='emuc') }},
        {{ trx_json_extract('src', ['emuc_kw'], type='varchar', alias='emuc_kw') }},
        {{ trx_json_extract('src', ['brep'], type='int', alias='brep') }},
        {{ trx_json_extract('src', ['brep_kw'], type='varchar', alias='brep_kw') }},
        {{ trx_json_extract('src', ['gtrf'], type='float', alias='gtrf') }},
        {{ trx_json_extract('src', ['iccc'], type='varchar', alias='iccc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs002') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'ccur']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
