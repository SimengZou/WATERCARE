{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tirou002 Machine table, Generated 2026-04-10T19:41:49Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['mcno'], type='varchar', alias='mcno') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['cwoc'], type='varchar', alias='cwoc') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['mcrt_1'], type='float', alias='mcrt_1') }},
        {{ trx_json_extract('src', ['mcrt_2'], type='float', alias='mcrt_2') }},
        {{ trx_json_extract('src', ['mcrt_3'], type='float', alias='mcrt_3') }},
        {{ trx_json_extract('src', ['cpcp'], type='varchar', alias='cpcp') }},
        {{ trx_json_extract('src', ['mccp'], type='float', alias='mccp') }},
        {{ trx_json_extract('src', ['mdcp'], type='float', alias='mdcp') }},
        {{ trx_json_extract('src', ['dque'], type='float', alias='dque') }},
        {{ trx_json_extract('src', ['mere'], type='int', alias='mere') }},
        {{ trx_json_extract('src', ['mere_kw'], type='varchar', alias='mere_kw') }},
        {{ trx_json_extract('src', ['mewo'], type='varchar', alias='mewo') }},
        {{ trx_json_extract('src', ['cwoc_ref_compnr'], type='int', alias='cwoc_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcp_ref_compnr'], type='int', alias='cpcp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tirou002') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'mcno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
