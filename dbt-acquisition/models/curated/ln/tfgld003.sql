{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld003 Group Company Parameters table, Generated 2026-04-10T19:41:39Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['indt'], type='timestamp_ntz', alias='indt') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['dhcu', 'en_US'], type='varchar', alias='dhcu__en_us') }},
        {{ trx_json_extract('src', ['dim1'], type='int', alias='dim1') }},
        {{ trx_json_extract('src', ['dim1_kw'], type='varchar', alias='dim1_kw') }},
        {{ trx_json_extract('src', ['dim2'], type='int', alias='dim2') }},
        {{ trx_json_extract('src', ['dim2_kw'], type='varchar', alias='dim2_kw') }},
        {{ trx_json_extract('src', ['dim3'], type='int', alias='dim3') }},
        {{ trx_json_extract('src', ['dim3_kw'], type='varchar', alias='dim3_kw') }},
        {{ trx_json_extract('src', ['dim4'], type='int', alias='dim4') }},
        {{ trx_json_extract('src', ['dim4_kw'], type='varchar', alias='dim4_kw') }},
        {{ trx_json_extract('src', ['dim5'], type='int', alias='dim5') }},
        {{ trx_json_extract('src', ['dim5_kw'], type='varchar', alias='dim5_kw') }},
        {{ trx_json_extract('src', ['dim6'], type='int', alias='dim6') }},
        {{ trx_json_extract('src', ['dim6_kw'], type='varchar', alias='dim6_kw') }},
        {{ trx_json_extract('src', ['dim7'], type='int', alias='dim7') }},
        {{ trx_json_extract('src', ['dim7_kw'], type='varchar', alias='dim7_kw') }},
        {{ trx_json_extract('src', ['dim8'], type='int', alias='dim8') }},
        {{ trx_json_extract('src', ['dim8_kw'], type='varchar', alias='dim8_kw') }},
        {{ trx_json_extract('src', ['dim9'], type='int', alias='dim9') }},
        {{ trx_json_extract('src', ['dim9_kw'], type='varchar', alias='dim9_kw') }},
        {{ trx_json_extract('src', ['dm10'], type='int', alias='dm10') }},
        {{ trx_json_extract('src', ['dm10_kw'], type='varchar', alias='dm10_kw') }},
        {{ trx_json_extract('src', ['dm11'], type='int', alias='dm11') }},
        {{ trx_json_extract('src', ['dm11_kw'], type='varchar', alias='dm11_kw') }},
        {{ trx_json_extract('src', ['dm12'], type='int', alias='dm12') }},
        {{ trx_json_extract('src', ['dm12_kw'], type='varchar', alias='dm12_kw') }},
        {{ trx_json_extract('src', ['rper'], type='int', alias='rper') }},
        {{ trx_json_extract('src', ['rper_kw'], type='varchar', alias='rper_kw') }},
        {{ trx_json_extract('src', ['nfpr'], type='int', alias='nfpr') }},
        {{ trx_json_extract('src', ['nrpr'], type='int', alias='nrpr') }},
        {{ trx_json_extract('src', ['nvpr'], type='int', alias='nvpr') }},
        {{ trx_json_extract('src', ['sdat'], type='int', alias='sdat') }},
        {{ trx_json_extract('src', ['bcmp'], type='int', alias='bcmp') }},
        {{ trx_json_extract('src', ['psep'], type='varchar', alias='psep') }},
        {{ trx_json_extract('src', ['cfst'], type='int', alias='cfst') }},
        {{ trx_json_extract('src', ['cfst_kw'], type='varchar', alias='cfst_kw') }},
        {{ trx_json_extract('src', ['dcfi'], type='int', alias='dcfi') }},
        {{ trx_json_extract('src', ['dcfi_kw'], type='varchar', alias='dcfi_kw') }},
        {{ trx_json_extract('src', ['papp'], type='int', alias='papp') }},
        {{ trx_json_extract('src', ['papp_kw'], type='varchar', alias='papp_kw') }},
        {{ trx_json_extract('src', ['gmbi'], type='int', alias='gmbi') }},
        {{ trx_json_extract('src', ['gmbi_kw'], type='varchar', alias='gmbi_kw') }},
        {{ trx_json_extract('src', ['sgmr'], type='int', alias='sgmr') }},
        {{ trx_json_extract('src', ['sgmr_kw'], type='varchar', alias='sgmr_kw') }},
        {{ trx_json_extract('src', ['psic'], type='int', alias='psic') }},
        {{ trx_json_extract('src', ['psic_kw'], type='varchar', alias='psic_kw') }},
        {{ trx_json_extract('src', ['psbk'], type='int', alias='psbk') }},
        {{ trx_json_extract('src', ['psbk_kw'], type='varchar', alias='psbk_kw') }},
        {{ trx_json_extract('src', ['pupt'], type='int', alias='pupt') }},
        {{ trx_json_extract('src', ['pupt_kw'], type='varchar', alias='pupt_kw') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld003') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'indt']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
