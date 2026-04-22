{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tdpur012 Purchase Offices table, Generated 2026-04-10T19:41:17Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cofc'], type='varchar', alias='cofc') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['ngpo'], type='varchar', alias='ngpo') }},
        {{ trx_json_extract('src', ['sepo'], type='varchar', alias='sepo') }},
        {{ trx_json_extract('src', ['seps'], type='varchar', alias='seps') }},
        {{ trx_json_extract('src', ['ngsp'], type='varchar', alias='ngsp') }},
        {{ trx_json_extract('src', ['sspo'], type='varchar', alias='sspo') }},
        {{ trx_json_extract('src', ['seqo'], type='varchar', alias='seqo') }},
        {{ trx_json_extract('src', ['safp'], type='varchar', alias='safp') }},
        {{ trx_json_extract('src', ['sequ'], type='varchar', alias='sequ') }},
        {{ trx_json_extract('src', ['ngpq'], type='varchar', alias='ngpq') }},
        {{ trx_json_extract('src', ['sepq'], type='varchar', alias='sepq') }},
        {{ trx_json_extract('src', ['ngpc'], type='varchar', alias='ngpc') }},
        {{ trx_json_extract('src', ['sepc'], type='varchar', alias='sepc') }},
        {{ trx_json_extract('src', ['ngrl'], type='varchar', alias='ngrl') }},
        {{ trx_json_extract('src', ['serl'], type='varchar', alias='serl') }},
        {{ trx_json_extract('src', ['ngpr'], type='varchar', alias='ngpr') }},
        {{ trx_json_extract('src', ['sepr'], type='varchar', alias='sepr') }},
        {{ trx_json_extract('src', ['ngco'], type='varchar', alias='ngco') }},
        {{ trx_json_extract('src', ['scid'], type='varchar', alias='scid') }},
        {{ trx_json_extract('src', ['cofc_ref_compnr'], type='int', alias='cofc_ref_compnr') }},
        {{ trx_json_extract('src', ['site_ref_compnr'], type='int', alias='site_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpo_sepo_ref_compnr'], type='int', alias='ngpo_sepo_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpo_seps_ref_compnr'], type='int', alias='ngpo_seps_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpo_ref_compnr'], type='int', alias='ngpo_ref_compnr') }},
        {{ trx_json_extract('src', ['ngsp_sspo_ref_compnr'], type='int', alias='ngsp_sspo_ref_compnr') }},
        {{ trx_json_extract('src', ['ngsp_seqo_ref_compnr'], type='int', alias='ngsp_seqo_ref_compnr') }},
        {{ trx_json_extract('src', ['ngsp_safp_ref_compnr'], type='int', alias='ngsp_safp_ref_compnr') }},
        {{ trx_json_extract('src', ['ngsp_sequ_ref_compnr'], type='int', alias='ngsp_sequ_ref_compnr') }},
        {{ trx_json_extract('src', ['ngsp_ref_compnr'], type='int', alias='ngsp_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpq_sepq_ref_compnr'], type='int', alias='ngpq_sepq_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpq_ref_compnr'], type='int', alias='ngpq_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpc_sepc_ref_compnr'], type='int', alias='ngpc_sepc_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpc_ref_compnr'], type='int', alias='ngpc_ref_compnr') }},
        {{ trx_json_extract('src', ['ngrl_serl_ref_compnr'], type='int', alias='ngrl_serl_ref_compnr') }},
        {{ trx_json_extract('src', ['ngrl_ref_compnr'], type='int', alias='ngrl_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpr_sepr_ref_compnr'], type='int', alias='ngpr_sepr_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpr_ref_compnr'], type='int', alias='ngpr_ref_compnr') }},
        {{ trx_json_extract('src', ['ngco_scid_ref_compnr'], type='int', alias='ngco_scid_ref_compnr') }},
        {{ trx_json_extract('src', ['ngco_ref_compnr'], type='int', alias='ngco_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tdpur012') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cofc']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
