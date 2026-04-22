{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinh310 Receipt Headers table, Generated 2026-04-10T19:42:48Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['rcno'], type='varchar', alias='rcno') }},
        {{ trx_json_extract('src', ['sfbp'], type='varchar', alias='sfbp') }},
        {{ trx_json_extract('src', ['carr'], type='varchar', alias='carr') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['shda'], type='varchar', alias='shda') }},
        {{ trx_json_extract('src', ['pddt'], type='timestamp_ntz', alias='pddt') }},
        {{ trx_json_extract('src', ['shdt'], type='timestamp_ntz', alias='shdt') }},
        {{ trx_json_extract('src', ['fval'], type='float', alias='fval') }},
        {{ trx_json_extract('src', ['curr'], type='varchar', alias='curr') }},
        {{ trx_json_extract('src', ['wght'], type='float', alias='wght') }},
        {{ trx_json_extract('src', ['cwun'], type='varchar', alias='cwun') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['dino', 'en_US'], type='varchar', alias='dino__en_us') }},
        {{ trx_json_extract('src', ['conf'], type='int', alias='conf') }},
        {{ trx_json_extract('src', ['conf_kw'], type='varchar', alias='conf_kw') }},
        {{ trx_json_extract('src', ['load'], type='varchar', alias='load') }},
        {{ trx_json_extract('src', ['shid'], type='varchar', alias='shid') }},
        {{ trx_json_extract('src', ['huid'], type='varchar', alias='huid') }},
        {{ trx_json_extract('src', ['imre', 'en_US'], type='varchar', alias='imre__en_us') }},
        {{ trx_json_extract('src', ['irdt'], type='timestamp_ntz', alias='irdt') }},
        {{ trx_json_extract('src', ['imrk', 'en_US'], type='varchar', alias='imrk__en_us') }},
        {{ trx_json_extract('src', ['port'], type='varchar', alias='port') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['tcap'], type='int', alias='tcap') }},
        {{ trx_json_extract('src', ['tcap_kw'], type='varchar', alias='tcap_kw') }},
        {{ trx_json_extract('src', ['tccp'], type='int', alias='tccp') }},
        {{ trx_json_extract('src', ['tccp_kw'], type='varchar', alias='tccp_kw') }},
        {{ trx_json_extract('src', ['trcd'], type='varchar', alias='trcd') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['sfbp_ref_compnr'], type='int', alias='sfbp_ref_compnr') }},
        {{ trx_json_extract('src', ['carr_ref_compnr'], type='int', alias='carr_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['shda_ref_compnr'], type='int', alias='shda_ref_compnr') }},
        {{ trx_json_extract('src', ['curr_ref_compnr'], type='int', alias='curr_ref_compnr') }},
        {{ trx_json_extract('src', ['cwun_ref_compnr'], type='int', alias='cwun_ref_compnr') }},
        {{ trx_json_extract('src', ['huid_ref_compnr'], type='int', alias='huid_ref_compnr') }},
        {{ trx_json_extract('src', ['port_ref_compnr'], type='int', alias='port_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinh310') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'rcno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
