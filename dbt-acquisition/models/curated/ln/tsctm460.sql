{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm460 Contract Revenues table, Generated 2026-04-10T19:42:34Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['csco'], type='varchar', alias='csco') }},
        {{ trx_json_extract('src', ['cchn'], type='int', alias='cchn') }},
        {{ trx_json_extract('src', ['cfln'], type='int', alias='cfln') }},
        {{ trx_json_extract('src', ['plyr'], type='int', alias='plyr') }},
        {{ trx_json_extract('src', ['plpr'], type='int', alias='plpr') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['ndrc'], type='int', alias='ndrc') }},
        {{ trx_json_extract('src', ['clrv'], type='float', alias='clrv') }},
        {{ trx_json_extract('src', ['corv'], type='float', alias='corv') }},
        {{ trx_json_extract('src', ['rahc_1'], type='float', alias='rahc_1') }},
        {{ trx_json_extract('src', ['rahc_2'], type='float', alias='rahc_2') }},
        {{ trx_json_extract('src', ['rahc_3'], type='float', alias='rahc_3') }},
        {{ trx_json_extract('src', ['ratc_1'], type='float', alias='ratc_1') }},
        {{ trx_json_extract('src', ['ratc_2'], type='float', alias='ratc_2') }},
        {{ trx_json_extract('src', ['ratc_3'], type='float', alias='ratc_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['ratd'], type='timestamp_ntz', alias='ratd') }},
        {{ trx_json_extract('src', ['rtor'], type='int', alias='rtor') }},
        {{ trx_json_extract('src', ['rtor_kw'], type='varchar', alias='rtor_kw') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['cmdt'], type='timestamp_ntz', alias='cmdt') }},
        {{ trx_json_extract('src', ['cmur'], type='varchar', alias='cmur') }},
        {{ trx_json_extract('src', ['rcdt'], type='timestamp_ntz', alias='rcdt') }},
        {{ trx_json_extract('src', ['rcur'], type='varchar', alias='rcur') }},
        {{ trx_json_extract('src', ['poyr'], type='int', alias='poyr') }},
        {{ trx_json_extract('src', ['popr'], type='int', alias='popr') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['erfa'], type='float', alias='erfa') }},
        {{ trx_json_extract('src', ['prov'], type='float', alias='prov') }},
        {{ trx_json_extract('src', ['rchn'], type='int', alias='rchn') }},
        {{ trx_json_extract('src', ['camt_1'], type='float', alias='camt_1') }},
        {{ trx_json_extract('src', ['camt_2'], type='float', alias='camt_2') }},
        {{ trx_json_extract('src', ['camt_3'], type='float', alias='camt_3') }},
        {{ trx_json_extract('src', ['acco_1'], type='float', alias='acco_1') }},
        {{ trx_json_extract('src', ['acco_2'], type='float', alias='acco_2') }},
        {{ trx_json_extract('src', ['acco_3'], type='float', alias='acco_3') }},
        {{ trx_json_extract('src', ['csco_cchn_ref_compnr'], type='int', alias='csco_cchn_ref_compnr') }},
        {{ trx_json_extract('src', ['csco_rchn_ref_compnr'], type='int', alias='csco_rchn_ref_compnr') }},
        {{ trx_json_extract('src', ['csco_ref_compnr'], type='int', alias='csco_ref_compnr') }},
        {{ trx_json_extract('src', ['corv_refc'], type='float', alias='corv_refc') }},
        {{ trx_json_extract('src', ['corv_dwhc'], type='float', alias='corv_dwhc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm460') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'csco', 'cchn', 'cfln', 'plyr', 'plpr', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
