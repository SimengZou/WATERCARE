{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcmcs065 Departments table, Generated 2026-04-10T19:41:13Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cwoc'], type='varchar', alias='cwoc') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['typd'], type='int', alias='typd') }},
        {{ trx_json_extract('src', ['typd_kw'], type='varchar', alias='typd_kw') }},
        {{ trx_json_extract('src', ['comp'], type='int', alias='comp') }},
        {{ trx_json_extract('src', ['ccal'], type='varchar', alias='ccal') }},
        {{ trx_json_extract('src', ['cadr'], type='varchar', alias='cadr') }},
        {{ trx_json_extract('src', ['city', 'en_US'], type='varchar', alias='city__en_us') }},
        {{ trx_json_extract('src', ['casi'], type='varchar', alias='casi') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['ract'], type='varchar', alias='ract') }},
        {{ trx_json_extract('src', ['clrt'], type='varchar', alias='clrt') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['dstp'], type='int', alias='dstp') }},
        {{ trx_json_extract('src', ['dstp_kw'], type='varchar', alias='dstp_kw') }},
        {{ trx_json_extract('src', ['ccal_ref_compnr'], type='int', alias='ccal_ref_compnr') }},
        {{ trx_json_extract('src', ['cadr_ref_compnr'], type='int', alias='cadr_ref_compnr') }},
        {{ trx_json_extract('src', ['casi_ref_compnr'], type='int', alias='casi_ref_compnr') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['ract_ref_compnr'], type='int', alias='ract_ref_compnr') }},
        {{ trx_json_extract('src', ['clrt_ref_compnr'], type='int', alias='clrt_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['cwoc_eunt'], type='varchar', alias='cwoc_eunt') }},
        {{ trx_json_extract('src', ['eunt_rcmp'], type='int', alias='eunt_rcmp') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcmcs065') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cwoc']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
