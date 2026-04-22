{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN txptc903 Change Request Header table, Generated 2026-04-10T19:42:43Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['creq'], type='varchar', alias='creq') }},
        {{ trx_json_extract('src', ['proj'], type='varchar', alias='proj') }},
        {{ trx_json_extract('src', ['prma'], type='varchar', alias='prma') }},
        {{ trx_json_extract('src', ['prph'], type='varchar', alias='prph') }},
        {{ trx_json_extract('src', ['pram'], type='float', alias='pram') }},
        {{ trx_json_extract('src', ['fapp'], type='varchar', alias='fapp') }},
        {{ trx_json_extract('src', ['requ'], type='varchar', alias='requ') }},
        {{ trx_json_extract('src', ['cdat'], type='timestamp_ntz', alias='cdat') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['sdat'], type='timestamp_ntz', alias='sdat') }},
        {{ trx_json_extract('src', ['suby'], type='varchar', alias='suby') }},
        {{ trx_json_extract('src', ['arda'], type='timestamp_ntz', alias='arda') }},
        {{ trx_json_extract('src', ['arby'], type='varchar', alias='arby') }},
        {{ trx_json_extract('src', ['fapr'], type='varchar', alias='fapr') }},
        {{ trx_json_extract('src', ['fadt'], type='timestamp_ntz', alias='fadt') }},
        {{ trx_json_extract('src', ['pmoa'], type='varchar', alias='pmoa') }},
        {{ trx_json_extract('src', ['padt'], type='timestamp_ntz', alias='padt') }},
        {{ trx_json_extract('src', ['vamt'], type='float', alias='vamt') }},
        {{ trx_json_extract('src', ['proj_ref_compnr'], type='int', alias='proj_ref_compnr') }},
        {{ trx_json_extract('src', ['prph_ref_compnr'], type='int', alias='prph_ref_compnr') }},
        {{ trx_json_extract('src', ['fapp_ref_compnr'], type='int', alias='fapp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_txptc903') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'creq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
