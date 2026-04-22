{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld010 Dimensions table, Generated 2026-04-10T19:41:41Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['dtyp'], type='int', alias='dtyp') }},
        {{ trx_json_extract('src', ['dimx'], type='varchar', alias='dimx') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['subl'], type='int', alias='subl') }},
        {{ trx_json_extract('src', ['pdix'], type='varchar', alias='pdix') }},
        {{ trx_json_extract('src', ['uni1'], type='int', alias='uni1') }},
        {{ trx_json_extract('src', ['uni1_kw'], type='varchar', alias='uni1_kw') }},
        {{ trx_json_extract('src', ['uni2'], type='int', alias='uni2') }},
        {{ trx_json_extract('src', ['uni2_kw'], type='varchar', alias='uni2_kw') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['pseq'], type='int', alias='pseq') }},
        {{ trx_json_extract('src', ['bloc'], type='int', alias='bloc') }},
        {{ trx_json_extract('src', ['bloc_kw'], type='varchar', alias='bloc_kw') }},
        {{ trx_json_extract('src', ['bfdt'], type='date', alias='bfdt') }},
        {{ trx_json_extract('src', ['budt'], type='date', alias='budt') }},
        {{ trx_json_extract('src', ['atyp'], type='int', alias='atyp') }},
        {{ trx_json_extract('src', ['atyp_kw'], type='varchar', alias='atyp_kw') }},
        {{ trx_json_extract('src', ['skey', 'en_US'], type='varchar', alias='skey__en_us') }},
        {{ trx_json_extract('src', ['qan1'], type='varchar', alias='qan1') }},
        {{ trx_json_extract('src', ['qan2'], type='varchar', alias='qan2') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['dtyp_pdix_ref_compnr'], type='int', alias='dtyp_pdix_ref_compnr') }},
        {{ trx_json_extract('src', ['dtyp_ref_compnr'], type='int', alias='dtyp_ref_compnr') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['qan1_ref_compnr'], type='int', alias='qan1_ref_compnr') }},
        {{ trx_json_extract('src', ['qan2_ref_compnr'], type='int', alias='qan2_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld010') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'dtyp', 'dimx']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
