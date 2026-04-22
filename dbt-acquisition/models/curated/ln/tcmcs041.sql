{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs041 Delivery Terms table, Generated 2026-04-10T19:41:11Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cdec'], type='varchar', alias='cdec') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['cdyn'], type='int', alias='cdyn') }},
        {{ trx_json_extract('src', ['cdyn_kw'], type='varchar', alias='cdyn_kw') }},
        {{ trx_json_extract('src', ['crpd'], type='int', alias='crpd') }},
        {{ trx_json_extract('src', ['crpd_kw'], type='varchar', alias='crpd_kw') }},
        {{ trx_json_extract('src', ['cptp'], type='int', alias='cptp') }},
        {{ trx_json_extract('src', ['cptp_kw'], type='varchar', alias='cptp_kw') }},
        {{ trx_json_extract('src', ['fcba'], type='varchar', alias='fcba') }},
        {{ trx_json_extract('src', ['potp'], type='int', alias='potp') }},
        {{ trx_json_extract('src', ['potp_kw'], type='varchar', alias='potp_kw') }},
        {{ trx_json_extract('src', ['tdgp'], type='int', alias='tdgp') }},
        {{ trx_json_extract('src', ['tdgp_kw'], type='varchar', alias='tdgp_kw') }},
        {{ trx_json_extract('src', ['spec'], type='int', alias='spec') }},
        {{ trx_json_extract('src', ['spec_kw'], type='varchar', alias='spec_kw') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['fcba_ref_compnr'], type='int', alias='fcba_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs041') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cdec']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
