{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcemm135 Clusters table, Generated 2026-04-10T19:41:06Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['clus'], type='varchar', alias='clus') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['extl'], type='int', alias='extl') }},
        {{ trx_json_extract('src', ['extl_kw'], type='varchar', alias='extl_kw') }},
        {{ trx_json_extract('src', ['hsub'], type='int', alias='hsub') }},
        {{ trx_json_extract('src', ['hsub_kw'], type='varchar', alias='hsub_kw') }},
        {{ trx_json_extract('src', ['cust'], type='int', alias='cust') }},
        {{ trx_json_extract('src', ['cust_kw'], type='varchar', alias='cust_kw') }},
        {{ trx_json_extract('src', ['ofbp'], type='varchar', alias='ofbp') }},
        {{ trx_json_extract('src', ['stbp'], type='varchar', alias='stbp') }},
        {{ trx_json_extract('src', ['expl'], type='int', alias='expl') }},
        {{ trx_json_extract('src', ['expl_kw'], type='varchar', alias='expl_kw') }},
        {{ trx_json_extract('src', ['ofbp_ref_compnr'], type='int', alias='ofbp_ref_compnr') }},
        {{ trx_json_extract('src', ['stbp_ref_compnr'], type='int', alias='stbp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcemm135') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'clus']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
