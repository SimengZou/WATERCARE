{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam820 Adjustment Transaction table, Generated 2026-04-10T19:41:36Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['tkey'], type='int', alias='tkey') }},
        {{ trx_json_extract('src', ['acmc_1'], type='float', alias='acmc_1') }},
        {{ trx_json_extract('src', ['acmc_2'], type='float', alias='acmc_2') }},
        {{ trx_json_extract('src', ['acmc_3'], type='float', alias='acmc_3') }},
        {{ trx_json_extract('src', ['acmz'], type='int', alias='acmz') }},
        {{ trx_json_extract('src', ['acmz_kw'], type='varchar', alias='acmz_kw') }},
        {{ trx_json_extract('src', ['apam_1'], type='float', alias='apam_1') }},
        {{ trx_json_extract('src', ['apam_2'], type='float', alias='apam_2') }},
        {{ trx_json_extract('src', ['apam_3'], type='float', alias='apam_3') }},
        {{ trx_json_extract('src', ['apaz'], type='int', alias='apaz') }},
        {{ trx_json_extract('src', ['apaz_kw'], type='varchar', alias='apaz_kw') }},
        {{ trx_json_extract('src', ['capi_1'], type='float', alias='capi_1') }},
        {{ trx_json_extract('src', ['capi_2'], type='float', alias='capi_2') }},
        {{ trx_json_extract('src', ['capi_3'], type='float', alias='capi_3') }},
        {{ trx_json_extract('src', ['capz'], type='int', alias='capz') }},
        {{ trx_json_extract('src', ['capz_kw'], type='varchar', alias='capz_kw') }},
        {{ trx_json_extract('src', ['cost_1'], type='float', alias='cost_1') }},
        {{ trx_json_extract('src', ['cost_2'], type='float', alias='cost_2') }},
        {{ trx_json_extract('src', ['cost_3'], type='float', alias='cost_3') }},
        {{ trx_json_extract('src', ['cosz'], type='int', alias='cosz') }},
        {{ trx_json_extract('src', ['cosz_kw'], type='varchar', alias='cosz_kw') }},
        {{ trx_json_extract('src', ['curr'], type='varchar', alias='curr') }},
        {{ trx_json_extract('src', ['itca_1'], type='float', alias='itca_1') }},
        {{ trx_json_extract('src', ['itca_2'], type='float', alias='itca_2') }},
        {{ trx_json_extract('src', ['itca_3'], type='float', alias='itca_3') }},
        {{ trx_json_extract('src', ['itcz'], type='int', alias='itcz') }},
        {{ trx_json_extract('src', ['itcz_kw'], type='varchar', alias='itcz_kw') }},
        {{ trx_json_extract('src', ['life'], type='int', alias='life') }},
        {{ trx_json_extract('src', ['lifz'], type='int', alias='lifz') }},
        {{ trx_json_extract('src', ['lifz_kw'], type='varchar', alias='lifz_kw') }},
        {{ trx_json_extract('src', ['ltdd_1'], type='float', alias='ltdd_1') }},
        {{ trx_json_extract('src', ['ltdd_2'], type='float', alias='ltdd_2') }},
        {{ trx_json_extract('src', ['ltdd_3'], type='float', alias='ltdd_3') }},
        {{ trx_json_extract('src', ['ltdz'], type='int', alias='ltdz') }},
        {{ trx_json_extract('src', ['ltdz_kw'], type='varchar', alias='ltdz_kw') }},
        {{ trx_json_extract('src', ['ratd'], type='timestamp_ntz', alias='ratd') }},
        {{ trx_json_extract('src', ['rate_1'], type='float', alias='rate_1') }},
        {{ trx_json_extract('src', ['rate_2'], type='float', alias='rate_2') }},
        {{ trx_json_extract('src', ['rate_3'], type='float', alias='rate_3') }},
        {{ trx_json_extract('src', ['ratf_1'], type='int', alias='ratf_1') }},
        {{ trx_json_extract('src', ['ratf_2'], type='int', alias='ratf_2') }},
        {{ trx_json_extract('src', ['ratf_3'], type='int', alias='ratf_3') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['reas'], type='varchar', alias='reas') }},
        {{ trx_json_extract('src', ['revl'], type='int', alias='revl') }},
        {{ trx_json_extract('src', ['revl_kw'], type='varchar', alias='revl_kw') }},
        {{ trx_json_extract('src', ['s179_1'], type='float', alias='s179_1') }},
        {{ trx_json_extract('src', ['s179_2'], type='float', alias='s179_2') }},
        {{ trx_json_extract('src', ['s179_3'], type='float', alias='s179_3') }},
        {{ trx_json_extract('src', ['s17z'], type='int', alias='s17z') }},
        {{ trx_json_extract('src', ['s17z_kw'], type='varchar', alias='s17z_kw') }},
        {{ trx_json_extract('src', ['salv_1'], type='float', alias='salv_1') }},
        {{ trx_json_extract('src', ['salv_2'], type='float', alias='salv_2') }},
        {{ trx_json_extract('src', ['salv_3'], type='float', alias='salv_3') }},
        {{ trx_json_extract('src', ['salz'], type='int', alias='salz') }},
        {{ trx_json_extract('src', ['salz_kw'], type='varchar', alias='salz_kw') }},
        {{ trx_json_extract('src', ['traf'], type='int', alias='traf') }},
        {{ trx_json_extract('src', ['traf_kw'], type='varchar', alias='traf_kw') }},
        {{ trx_json_extract('src', ['user'], type='varchar', alias='user') }},
        {{ trx_json_extract('src', ['tkey_ref_compnr'], type='int', alias='tkey_ref_compnr') }},
        {{ trx_json_extract('src', ['reas_ref_compnr'], type='int', alias='reas_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam820') }}
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
