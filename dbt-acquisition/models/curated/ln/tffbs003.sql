{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffbs003 Budget table, Generated 2026-04-10T19:41:37Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['budg'], type='varchar', alias='budg') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['sdbu'], type='int', alias='sdbu') }},
        {{ trx_json_extract('src', ['sdbu_kw'], type='varchar', alias='sdbu_kw') }},
        {{ trx_json_extract('src', ['budm'], type='int', alias='budm') }},
        {{ trx_json_extract('src', ['budm_kw'], type='varchar', alias='budm_kw') }},
        {{ trx_json_extract('src', ['llac'], type='int', alias='llac') }},
        {{ trx_json_extract('src', ['llac_kw'], type='varchar', alias='llac_kw') }},
        {{ trx_json_extract('src', ['adt1'], type='int', alias='adt1') }},
        {{ trx_json_extract('src', ['adt1_kw'], type='varchar', alias='adt1_kw') }},
        {{ trx_json_extract('src', ['adt2'], type='int', alias='adt2') }},
        {{ trx_json_extract('src', ['adt2_kw'], type='varchar', alias='adt2_kw') }},
        {{ trx_json_extract('src', ['adt3'], type='int', alias='adt3') }},
        {{ trx_json_extract('src', ['adt3_kw'], type='varchar', alias='adt3_kw') }},
        {{ trx_json_extract('src', ['adt4'], type='int', alias='adt4') }},
        {{ trx_json_extract('src', ['adt4_kw'], type='varchar', alias='adt4_kw') }},
        {{ trx_json_extract('src', ['adt5'], type='int', alias='adt5') }},
        {{ trx_json_extract('src', ['adt5_kw'], type='varchar', alias='adt5_kw') }},
        {{ trx_json_extract('src', ['adt6'], type='int', alias='adt6') }},
        {{ trx_json_extract('src', ['adt6_kw'], type='varchar', alias='adt6_kw') }},
        {{ trx_json_extract('src', ['adt7'], type='int', alias='adt7') }},
        {{ trx_json_extract('src', ['adt7_kw'], type='varchar', alias='adt7_kw') }},
        {{ trx_json_extract('src', ['adt8'], type='int', alias='adt8') }},
        {{ trx_json_extract('src', ['adt8_kw'], type='varchar', alias='adt8_kw') }},
        {{ trx_json_extract('src', ['adt9'], type='int', alias='adt9') }},
        {{ trx_json_extract('src', ['adt9_kw'], type='varchar', alias='adt9_kw') }},
        {{ trx_json_extract('src', ['ad10'], type='int', alias='ad10') }},
        {{ trx_json_extract('src', ['ad10_kw'], type='varchar', alias='ad10_kw') }},
        {{ trx_json_extract('src', ['ad11'], type='int', alias='ad11') }},
        {{ trx_json_extract('src', ['ad11_kw'], type='varchar', alias='ad11_kw') }},
        {{ trx_json_extract('src', ['ad12'], type='int', alias='ad12') }},
        {{ trx_json_extract('src', ['ad12_kw'], type='varchar', alias='ad12_kw') }},
        {{ trx_json_extract('src', ['aqu1'], type='int', alias='aqu1') }},
        {{ trx_json_extract('src', ['aqu1_kw'], type='varchar', alias='aqu1_kw') }},
        {{ trx_json_extract('src', ['aqu2'], type='int', alias='aqu2') }},
        {{ trx_json_extract('src', ['aqu2_kw'], type='varchar', alias='aqu2_kw') }},
        {{ trx_json_extract('src', ['nbpr'], type='int', alias='nbpr') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffbs003') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'budg']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
