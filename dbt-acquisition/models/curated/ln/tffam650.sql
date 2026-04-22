{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam650 Reasons table, Generated 2026-04-10T19:41:33Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['reas'], type='varchar', alias='reas') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['rety'], type='int', alias='rety') }},
        {{ trx_json_extract('src', ['rety_kw'], type='varchar', alias='rety_kw') }},
        {{ trx_json_extract('src', ['cpga'], type='int', alias='cpga') }},
        {{ trx_json_extract('src', ['cpga_kw'], type='varchar', alias='cpga_kw') }},
        {{ trx_json_extract('src', ['impa'], type='int', alias='impa') }},
        {{ trx_json_extract('src', ['impa_kw'], type='varchar', alias='impa_kw') }},
        {{ trx_json_extract('src', ['skev'], type='int', alias='skev') }},
        {{ trx_json_extract('src', ['skev_kw'], type='varchar', alias='skev_kw') }},
        {{ trx_json_extract('src', ['czev'], type='int', alias='czev') }},
        {{ trx_json_extract('src', ['czev_kw'], type='varchar', alias='czev_kw') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam650') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'reas']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
