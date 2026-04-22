{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Auto-generated model',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['Version'], type='varchar', alias='version') }},
        {{ trx_json_extract('src', ['Year'], type='varchar', alias='year') }},
        {{ trx_json_extract('src', ['Period'], type='varchar', alias='period') }},
        {{ trx_json_extract('src', ['LdgTrxType'], type='varchar', alias='ldgtrxtype') }},
        {{ trx_json_extract('src', ['Currency'], type='varchar', alias='currency') }},
        {{ trx_json_extract('src', ['Analysis01'], type='varchar', alias='analysis01') }},
        {{ trx_json_extract('src', ['Analysis02'], type='varchar', alias='analysis02') }},
        {{ trx_json_extract('src', ['Analysis03'], type='varchar', alias='analysis03') }},
        {{ trx_json_extract('src', ['Analysis04'], type='varchar', alias='analysis04') }},
        {{ trx_json_extract('src', ['Analysis05'], type='varchar', alias='analysis05') }},
        {{ trx_json_extract('src', ['Analysis06'], type='varchar', alias='analysis06') }},
        {{ trx_json_extract('src', ['Analysis07'], type='varchar', alias='analysis07') }},
        {{ trx_json_extract('src', ['Analysis08'], type='varchar', alias='analysis08') }},
        {{ trx_json_extract('src', ['Analysis09'], type='varchar', alias='analysis09') }},
        {{ trx_json_extract('src', ['Analysis10'], type='varchar', alias='analysis10') }},
        {{ trx_json_extract('src', ['Account'], type='varchar', alias='_account') }},
        {{ trx_json_extract('src', ['LedgerMeas'], type='varchar', alias='ledgermeas') }},
        {{ trx_json_extract('src', ['Value'], type='varchar', alias='value') }},
        {{ trx_json_extract('src', ['Timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_cube_ledger') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['version', 'year', 'period', 'ldgtrxtype', 'currency', 'analysis01', 'analysis02', 'analysis03', 'analysis04', 'analysis05', 'analysis06', 'analysis07', 'analysis08', 'analysis09', 'analysis10', '_account', 'ledgermeas']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['timestamp']) }}
