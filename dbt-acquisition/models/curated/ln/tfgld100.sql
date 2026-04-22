{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld100 Financial Batch table, Generated 2026-04-10T19:41:42Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['btno'], type='int', alias='btno') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['tedt'], type='date', alias='tedt') }},
        {{ trx_json_extract('src', ['trdt'], type='date', alias='trdt') }},
        {{ trx_json_extract('src', ['itbc'], type='int', alias='itbc') }},
        {{ trx_json_extract('src', ['itbc_kw'], type='varchar', alias='itbc_kw') }},
        {{ trx_json_extract('src', ['fprd'], type='int', alias='fprd') }},
        {{ trx_json_extract('src', ['rprd'], type='int', alias='rprd') }},
        {{ trx_json_extract('src', ['vprd'], type='int', alias='vprd') }},
        {{ trx_json_extract('src', ['vyer'], type='int', alias='vyer') }},
        {{ trx_json_extract('src', ['bref', 'en_US'], type='varchar', alias='bref__en_us') }},
        {{ trx_json_extract('src', ['btyp'], type='int', alias='btyp') }},
        {{ trx_json_extract('src', ['btyp_kw'], type='varchar', alias='btyp_kw') }},
        {{ trx_json_extract('src', ['trun'], type='int', alias='trun') }},
        {{ trx_json_extract('src', ['tstt'], type='int', alias='tstt') }},
        {{ trx_json_extract('src', ['tstt_kw'], type='varchar', alias='tstt_kw') }},
        {{ trx_json_extract('src', ['selt'], type='int', alias='selt') }},
        {{ trx_json_extract('src', ['selt_kw'], type='varchar', alias='selt_kw') }},
        {{ trx_json_extract('src', ['year_ref_compnr'], type='int', alias='year_ref_compnr') }},
        {{ trx_json_extract('src', ['vyer_ref_compnr'], type='int', alias='vyer_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld100') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'year', 'btno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
