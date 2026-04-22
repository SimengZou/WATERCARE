{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam800 Transaction table, Generated 2026-04-10T19:41:34Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['tkey'], type='int', alias='tkey') }},
        {{ trx_json_extract('src', ['anbr'], type='varchar', alias='anbr') }},
        {{ trx_json_extract('src', ['aext'], type='varchar', alias='aext') }},
        {{ trx_json_extract('src', ['amnt_1'], type='float', alias='amnt_1') }},
        {{ trx_json_extract('src', ['amnt_2'], type='float', alias='amnt_2') }},
        {{ trx_json_extract('src', ['amnt_3'], type='float', alias='amnt_3') }},
        {{ trx_json_extract('src', ['atty'], type='int', alias='atty') }},
        {{ trx_json_extract('src', ['atty_kw'], type='varchar', alias='atty_kw') }},
        {{ trx_json_extract('src', ['book'], type='varchar', alias='book') }},
        {{ trx_json_extract('src', ['docn'], type='int', alias='docn') }},
        {{ trx_json_extract('src', ['line'], type='int', alias='line') }},
        {{ trx_json_extract('src', ['rort'], type='int', alias='rort') }},
        {{ trx_json_extract('src', ['rort_kw'], type='varchar', alias='rort_kw') }},
        {{ trx_json_extract('src', ['prod'], type='int', alias='prod') }},
        {{ trx_json_extract('src', ['post'], type='int', alias='post') }},
        {{ trx_json_extract('src', ['post_kw'], type='varchar', alias='post_kw') }},
        {{ trx_json_extract('src', ['tdat'], type='date', alias='tdat') }},
        {{ trx_json_extract('src', ['ttyp'], type='varchar', alias='ttyp') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['anbr_aext_book_ref_compnr'], type='int', alias='anbr_aext_book_ref_compnr') }},
        {{ trx_json_extract('src', ['anbr_aext_ref_compnr'], type='int', alias='anbr_aext_ref_compnr') }},
        {{ trx_json_extract('src', ['book_ref_compnr'], type='int', alias='book_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam800') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'tkey']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
