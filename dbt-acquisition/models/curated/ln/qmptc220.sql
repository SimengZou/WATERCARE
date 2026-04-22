{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN qmptc220 Storage Inspections table, Generated 2022-06-15T01:21:06Z from package combination ce01055',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['effn'], type='int', alias='effn') }},
        {{ trx_json_extract('src', ['revi'], type='varchar', alias='revi') }},
        {{ trx_json_extract('src', ['seqn'], type='int', alias='seqn') }},
        {{ trx_json_extract('src', ['oqua'], type='float', alias='oqua') }},
        {{ trx_json_extract('src', ['raqa'], type='float', alias='raqa') }},
        {{ trx_json_extract('src', ['rdqa'], type='float', alias='rdqa') }},
        {{ trx_json_extract('src', ['rrqa'], type='float', alias='rrqa') }},
        {{ trx_json_extract('src', ['aaqa'], type='float', alias='aaqa') }},
        {{ trx_json_extract('src', ['arqa'], type='float', alias='arqa') }},
        {{ trx_json_extract('src', ['cdis'], type='varchar', alias='cdis') }},
        {{ trx_json_extract('src', ['rsta'], type='int', alias='rsta') }},
        {{ trx_json_extract('src', ['rsta_kw'], type='varchar', alias='rsta_kw') }},
        {{ trx_json_extract('src', ['cdat'], type='timestamp_ntz', alias='cdat') }},
        {{ trx_json_extract('src', ['pdat'], type='timestamp_ntz', alias='pdat') }},
        {{ trx_json_extract('src', ['gncm'], type='int', alias='gncm') }},
        {{ trx_json_extract('src', ['gncm_kw'], type='varchar', alias='gncm_kw') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['acdt'], type='timestamp_ntz', alias='acdt') }},
        {{ trx_json_extract('src', ['cldt'], type='timestamp_ntz', alias='cldt') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['effn_ref_compnr'], type='int', alias='effn_ref_compnr') }},
        {{ trx_json_extract('src', ['cdis_ref_compnr'], type='int', alias='cdis_ref_compnr') }},
        {{ trx_json_extract('src', ['oqua_buar'], type='float', alias='oqua_buar') }},
        {{ trx_json_extract('src', ['oqua_buln'], type='float', alias='oqua_buln') }},
        {{ trx_json_extract('src', ['oqua_bupc'], type='float', alias='oqua_bupc') }},
        {{ trx_json_extract('src', ['oqua_butm'], type='float', alias='oqua_butm') }},
        {{ trx_json_extract('src', ['oqua_buvl'], type='float', alias='oqua_buvl') }},
        {{ trx_json_extract('src', ['oqua_buwg'], type='float', alias='oqua_buwg') }},
        {{ trx_json_extract('src', ['raqa_buar'], type='float', alias='raqa_buar') }},
        {{ trx_json_extract('src', ['raqa_buln'], type='float', alias='raqa_buln') }},
        {{ trx_json_extract('src', ['raqa_bupc'], type='float', alias='raqa_bupc') }},
        {{ trx_json_extract('src', ['raqa_butm'], type='float', alias='raqa_butm') }},
        {{ trx_json_extract('src', ['raqa_buvl'], type='float', alias='raqa_buvl') }},
        {{ trx_json_extract('src', ['raqa_buwg'], type='float', alias='raqa_buwg') }},
        {{ trx_json_extract('src', ['rrqa_buar'], type='float', alias='rrqa_buar') }},
        {{ trx_json_extract('src', ['rrqa_buln'], type='float', alias='rrqa_buln') }},
        {{ trx_json_extract('src', ['rrqa_bupc'], type='float', alias='rrqa_bupc') }},
        {{ trx_json_extract('src', ['rrqa_butm'], type='float', alias='rrqa_butm') }},
        {{ trx_json_extract('src', ['rrqa_buvl'], type='float', alias='rrqa_buvl') }},
        {{ trx_json_extract('src', ['rrqa_buwg'], type='float', alias='rrqa_buwg') }},
        {{ trx_json_extract('src', ['rdqa_buar'], type='float', alias='rdqa_buar') }},
        {{ trx_json_extract('src', ['rdqa_buln'], type='float', alias='rdqa_buln') }},
        {{ trx_json_extract('src', ['rdqa_bupc'], type='float', alias='rdqa_bupc') }},
        {{ trx_json_extract('src', ['rdqa_butm'], type='float', alias='rdqa_butm') }},
        {{ trx_json_extract('src', ['rdqa_buvl'], type='float', alias='rdqa_buvl') }},
        {{ trx_json_extract('src', ['rdqa_buwg'], type='float', alias='rdqa_buwg') }},
        {{ trx_json_extract('src', ['aaqa_buar'], type='float', alias='aaqa_buar') }},
        {{ trx_json_extract('src', ['aaqa_buln'], type='float', alias='aaqa_buln') }},
        {{ trx_json_extract('src', ['aaqa_bupc'], type='float', alias='aaqa_bupc') }},
        {{ trx_json_extract('src', ['aaqa_butm'], type='float', alias='aaqa_butm') }},
        {{ trx_json_extract('src', ['aaqa_buvl'], type='float', alias='aaqa_buvl') }},
        {{ trx_json_extract('src', ['aaqa_buwg'], type='float', alias='aaqa_buwg') }},
        {{ trx_json_extract('src', ['arqa_buar'], type='float', alias='arqa_buar') }},
        {{ trx_json_extract('src', ['arqa_buln'], type='float', alias='arqa_buln') }},
        {{ trx_json_extract('src', ['arqa_bupc'], type='float', alias='arqa_bupc') }},
        {{ trx_json_extract('src', ['arqa_butm'], type='float', alias='arqa_butm') }},
        {{ trx_json_extract('src', ['arqa_buvl'], type='float', alias='arqa_buvl') }},
        {{ trx_json_extract('src', ['arqa_buwg'], type='float', alias='arqa_buwg') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_qmptc220') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'item', 'effn', 'revi', 'seqn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
