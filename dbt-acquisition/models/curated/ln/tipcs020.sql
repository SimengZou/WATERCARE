{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tipcs020 General Project Data table, Generated 2026-04-10T19:41:47Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['refe', 'en_US'], type='varchar', alias='refe__en_us') }},
        {{ trx_json_extract('src', ['seak', 'en_US'], type='varchar', alias='seak__en_us') }},
        {{ trx_json_extract('src', ['kopr'], type='int', alias='kopr') }},
        {{ trx_json_extract('src', ['kopr_kw'], type='varchar', alias='kopr_kw') }},
        {{ trx_json_extract('src', ['ccgr'], type='varchar', alias='ccgr') }},
        {{ trx_json_extract('src', ['bpid'], type='varchar', alias='bpid') }},
        {{ trx_json_extract('src', ['pemp'], type='varchar', alias='pemp') }},
        {{ trx_json_extract('src', ['ffci'], type='int', alias='ffci') }},
        {{ trx_json_extract('src', ['cpcc'], type='varchar', alias='cpcc') }},
        {{ trx_json_extract('src', ['clco'], type='varchar', alias='clco') }},
        {{ trx_json_extract('src', ['peng'], type='int', alias='peng') }},
        {{ trx_json_extract('src', ['peng_kw'], type='varchar', alias='peng_kw') }},
        {{ trx_json_extract('src', ['cfdt'], type='timestamp_ntz', alias='cfdt') }},
        {{ trx_json_extract('src', ['dtfs'], type='int', alias='dtfs') }},
        {{ trx_json_extract('src', ['dtfs_kw'], type='varchar', alias='dtfs_kw') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['ccgr_ref_compnr'], type='int', alias='ccgr_ref_compnr') }},
        {{ trx_json_extract('src', ['bpid_ref_compnr'], type='int', alias='bpid_ref_compnr') }},
        {{ trx_json_extract('src', ['pemp_ref_compnr'], type='int', alias='pemp_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcc_ref_compnr'], type='int', alias='cpcc_ref_compnr') }},
        {{ trx_json_extract('src', ['clco_ref_compnr'], type='int', alias='clco_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tipcs020') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cprj']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
