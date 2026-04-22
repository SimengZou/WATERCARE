{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld007 Period Status table, Generated 2026-04-10T19:41:41Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['ptyp'], type='int', alias='ptyp') }},
        {{ trx_json_extract('src', ['ptyp_kw'], type='varchar', alias='ptyp_kw') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['prno'], type='int', alias='prno') }},
        {{ trx_json_extract('src', ['sacp'], type='int', alias='sacp') }},
        {{ trx_json_extract('src', ['sacp_kw'], type='varchar', alias='sacp_kw') }},
        {{ trx_json_extract('src', ['dacp'], type='timestamp_ntz', alias='dacp') }},
        {{ trx_json_extract('src', ['uacp'], type='varchar', alias='uacp') }},
        {{ trx_json_extract('src', ['sacr'], type='int', alias='sacr') }},
        {{ trx_json_extract('src', ['sacr_kw'], type='varchar', alias='sacr_kw') }},
        {{ trx_json_extract('src', ['dacr'], type='timestamp_ntz', alias='dacr') }},
        {{ trx_json_extract('src', ['uacr'], type='varchar', alias='uacr') }},
        {{ trx_json_extract('src', ['scmg'], type='int', alias='scmg') }},
        {{ trx_json_extract('src', ['scmg_kw'], type='varchar', alias='scmg_kw') }},
        {{ trx_json_extract('src', ['dcmg'], type='timestamp_ntz', alias='dcmg') }},
        {{ trx_json_extract('src', ['ucmg'], type='varchar', alias='ucmg') }},
        {{ trx_json_extract('src', ['sgld'], type='int', alias='sgld') }},
        {{ trx_json_extract('src', ['sgld_kw'], type='varchar', alias='sgld_kw') }},
        {{ trx_json_extract('src', ['dgld'], type='timestamp_ntz', alias='dgld') }},
        {{ trx_json_extract('src', ['ugld'], type='varchar', alias='ugld') }},
        {{ trx_json_extract('src', ['sint'], type='int', alias='sint') }},
        {{ trx_json_extract('src', ['sint_kw'], type='varchar', alias='sint_kw') }},
        {{ trx_json_extract('src', ['dint'], type='timestamp_ntz', alias='dint') }},
        {{ trx_json_extract('src', ['uint'], type='varchar', alias='uint') }},
        {{ trx_json_extract('src', ['ptyp_year_prno_ref_compnr'], type='int', alias='ptyp_year_prno_ref_compnr') }},
        {{ trx_json_extract('src', ['year_ref_compnr'], type='int', alias='year_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld007') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'ptyp', 'year', 'prno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
