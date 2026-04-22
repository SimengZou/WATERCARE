{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm480 Contract Cost Coverage - Overview table, Generated 2026-04-10T19:42:35Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['csco'], type='varchar', alias='csco') }},
        {{ trx_json_extract('src', ['cchn'], type='int', alias='cchn') }},
        {{ trx_json_extract('src', ['cfln'], type='int', alias='cfln') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['ivsq'], type='int', alias='ivsq') }},
        {{ trx_json_extract('src', ['acco_1'], type='float', alias='acco_1') }},
        {{ trx_json_extract('src', ['acco_2'], type='float', alias='acco_2') }},
        {{ trx_json_extract('src', ['acco_3'], type='float', alias='acco_3') }},
        {{ trx_json_extract('src', ['codt'], type='timestamp_ntz', alias='codt') }},
        {{ trx_json_extract('src', ['plyr'], type='int', alias='plyr') }},
        {{ trx_json_extract('src', ['plpr'], type='int', alias='plpr') }},
        {{ trx_json_extract('src', ['ortp'], type='int', alias='ortp') }},
        {{ trx_json_extract('src', ['ortp_kw'], type='varchar', alias='ortp_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['cvln'], type='int', alias='cvln') }},
        {{ trx_json_extract('src', ['cotp'], type='int', alias='cotp') }},
        {{ trx_json_extract('src', ['cotp_kw'], type='varchar', alias='cotp_kw') }},
        {{ trx_json_extract('src', ['poyr'], type='int', alias='poyr') }},
        {{ trx_json_extract('src', ['popr'], type='int', alias='popr') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['ircl'], type='int', alias='ircl') }},
        {{ trx_json_extract('src', ['ircl_kw'], type='varchar', alias='ircl_kw') }},
        {{ trx_json_extract('src', ['rchn'], type='int', alias='rchn') }},
        {{ trx_json_extract('src', ['csco_cchn_ref_compnr'], type='int', alias='csco_cchn_ref_compnr') }},
        {{ trx_json_extract('src', ['csco_rchn_ref_compnr'], type='int', alias='csco_rchn_ref_compnr') }},
        {{ trx_json_extract('src', ['csco_ref_compnr'], type='int', alias='csco_ref_compnr') }},
        {{ trx_json_extract('src', ['acco_cntc'], type='float', alias='acco_cntc') }},
        {{ trx_json_extract('src', ['acco_refc'], type='float', alias='acco_refc') }},
        {{ trx_json_extract('src', ['acco_dwhc'], type='float', alias='acco_dwhc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm480') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'csco', 'cchn', 'cfln', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
