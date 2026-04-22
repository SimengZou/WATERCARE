{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tdpur094 Purchase Order Types table, Generated 2026-04-10T19:41:17Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['potp'], type='varchar', alias='potp') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['drct'], type='int', alias='drct') }},
        {{ trx_json_extract('src', ['drct_kw'], type='varchar', alias='drct_kw') }},
        {{ trx_json_extract('src', ['reto'], type='int', alias='reto') }},
        {{ trx_json_extract('src', ['reto_kw'], type='varchar', alias='reto_kw') }},
        {{ trx_json_extract('src', ['coun'], type='int', alias='coun') }},
        {{ trx_json_extract('src', ['coun_kw'], type='varchar', alias='coun_kw') }},
        {{ trx_json_extract('src', ['sund'], type='int', alias='sund') }},
        {{ trx_json_extract('src', ['sund_kw'], type='varchar', alias='sund_kw') }},
        {{ trx_json_extract('src', ['cnsp'], type='int', alias='cnsp') }},
        {{ trx_json_extract('src', ['cnsp_kw'], type='varchar', alias='cnsp_kw') }},
        {{ trx_json_extract('src', ['cnsr'], type='int', alias='cnsr') }},
        {{ trx_json_extract('src', ['cnsr_kw'], type='varchar', alias='cnsr_kw') }},
        {{ trx_json_extract('src', ['subc'], type='int', alias='subc') }},
        {{ trx_json_extract('src', ['subc_kw'], type='varchar', alias='subc_kw') }},
        {{ trx_json_extract('src', ['cfnm'], type='int', alias='cfnm') }},
        {{ trx_json_extract('src', ['cfnm_kw'], type='varchar', alias='cfnm_kw') }},
        {{ trx_json_extract('src', ['wrhp'], type='varchar', alias='wrhp') }},
        {{ trx_json_extract('src', ['slcp'], type='int', alias='slcp') }},
        {{ trx_json_extract('src', ['slcp_kw'], type='varchar', alias='slcp_kw') }},
        {{ trx_json_extract('src', ['cbor'], type='int', alias='cbor') }},
        {{ trx_json_extract('src', ['cbor_kw'], type='varchar', alias='cbor_kw') }},
        {{ trx_json_extract('src', ['etpc'], type='int', alias='etpc') }},
        {{ trx_json_extract('src', ['etpc_kw'], type='varchar', alias='etpc_kw') }},
        {{ trx_json_extract('src', ['ngpo'], type='varchar', alias='ngpo') }},
        {{ trx_json_extract('src', ['sepo'], type='varchar', alias='sepo') }},
        {{ trx_json_extract('src', ['ngpq'], type='varchar', alias='ngpq') }},
        {{ trx_json_extract('src', ['sepq'], type='varchar', alias='sepq') }},
        {{ trx_json_extract('src', ['ngpc'], type='varchar', alias='ngpc') }},
        {{ trx_json_extract('src', ['sepc'], type='varchar', alias='sepc') }},
        {{ trx_json_extract('src', ['efdt'], type='timestamp_ntz', alias='efdt') }},
        {{ trx_json_extract('src', ['exdt'], type='timestamp_ntz', alias='exdt') }},
        {{ trx_json_extract('src', ['proc'], type='varchar', alias='proc') }},
        {{ trx_json_extract('src', ['pmsk'], type='varchar', alias='pmsk') }},
        {{ trx_json_extract('src', ['ngpo_sepo_ref_compnr'], type='int', alias='ngpo_sepo_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpo_ref_compnr'], type='int', alias='ngpo_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpq_sepq_ref_compnr'], type='int', alias='ngpq_sepq_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpq_ref_compnr'], type='int', alias='ngpq_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpc_sepc_ref_compnr'], type='int', alias='ngpc_sepc_ref_compnr') }},
        {{ trx_json_extract('src', ['ngpc_ref_compnr'], type='int', alias='ngpc_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tdpur094') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'potp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
