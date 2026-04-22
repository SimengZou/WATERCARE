{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcccp060 Period Tables table, Generated 2026-04-10T19:40:59Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cpdt'], type='varchar', alias='cpdt') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['dura'], type='int', alias='dura') }},
        {{ trx_json_extract('src', ['dtyp'], type='int', alias='dtyp') }},
        {{ trx_json_extract('src', ['dtyp_kw'], type='varchar', alias='dtyp_kw') }},
        {{ trx_json_extract('src', ['sttm'], type='int', alias='sttm') }},
        {{ trx_json_extract('src', ['ychk'], type='int', alias='ychk') }},
        {{ trx_json_extract('src', ['ychk_kw'], type='varchar', alias='ychk_kw') }},
        {{ trx_json_extract('src', ['mchk'], type='int', alias='mchk') }},
        {{ trx_json_extract('src', ['mchk_kw'], type='varchar', alias='mchk_kw') }},
        {{ trx_json_extract('src', ['gchk'], type='int', alias='gchk') }},
        {{ trx_json_extract('src', ['gchk_kw'], type='varchar', alias='gchk_kw') }},
        {{ trx_json_extract('src', ['tzid'], type='varchar', alias='tzid') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcccp060') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cpdt']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
