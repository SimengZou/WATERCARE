{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tcgen000 Generic Parameters table, Generated 2026-04-10T19:41:07Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['indt'], type='timestamp_ntz', alias='indt') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['awdt'], type='int', alias='awdt') }},
        {{ trx_json_extract('src', ['awdt_kw'], type='varchar', alias='awdt_kw') }},
        {{ trx_json_extract('src', ['wdto', 'en_US'], type='varchar', alias='wdto__en_us') }},
        {{ trx_json_extract('src', ['wdti', 'en_US'], type='varchar', alias='wdti__en_us') }},
        {{ trx_json_extract('src', ['wdte', 'en_US'], type='varchar', alias='wdte__en_us') }},
        {{ trx_json_extract('src', ['wdtr', 'en_US'], type='varchar', alias='wdtr__en_us') }},
        {{ trx_json_extract('src', ['awfa'], type='int', alias='awfa') }},
        {{ trx_json_extract('src', ['awfa_kw'], type='varchar', alias='awfa_kw') }},
        {{ trx_json_extract('src', ['wfao', 'en_US'], type='varchar', alias='wfao__en_us') }},
        {{ trx_json_extract('src', ['wfai', 'en_US'], type='varchar', alias='wfai__en_us') }},
        {{ trx_json_extract('src', ['wfae', 'en_US'], type='varchar', alias='wfae__en_us') }},
        {{ trx_json_extract('src', ['wfar', 'en_US'], type='varchar', alias='wfar__en_us') }},
        {{ trx_json_extract('src', ['time'], type='int', alias='time') }},
        {{ trx_json_extract('src', ['crpd'], type='int', alias='crpd') }},
        {{ trx_json_extract('src', ['clcd'], type='timestamp_ntz', alias='clcd') }},
        {{ trx_json_extract('src', ['cvfd'], type='timestamp_ntz', alias='cvfd') }},
        {{ trx_json_extract('src', ['dcur'], type='varchar', alias='dcur') }},
        {{ trx_json_extract('src', ['rtyp'], type='varchar', alias='rtyp') }},
        {{ trx_json_extract('src', ['cpie'], type='varchar', alias='cpie') }},
        {{ trx_json_extract('src', ['dcmp'], type='int', alias='dcmp') }},
        {{ trx_json_extract('src', ['urtp'], type='int', alias='urtp') }},
        {{ trx_json_extract('src', ['urtp_kw'], type='varchar', alias='urtp_kw') }},
        {{ trx_json_extract('src', ['udwc'], type='int', alias='udwc') }},
        {{ trx_json_extract('src', ['udwc_kw'], type='varchar', alias='udwc_kw') }},
        {{ trx_json_extract('src', ['urfc'], type='int', alias='urfc') }},
        {{ trx_json_extract('src', ['urfc_kw'], type='varchar', alias='urfc_kw') }},
        {{ trx_json_extract('src', ['uslc'], type='int', alias='uslc') }},
        {{ trx_json_extract('src', ['uslc_kw'], type='varchar', alias='uslc_kw') }},
        {{ trx_json_extract('src', ['urc1'], type='int', alias='urc1') }},
        {{ trx_json_extract('src', ['urc1_kw'], type='varchar', alias='urc1_kw') }},
        {{ trx_json_extract('src', ['urc2'], type='int', alias='urc2') }},
        {{ trx_json_extract('src', ['urc2_kw'], type='varchar', alias='urc2_kw') }},
        {{ trx_json_extract('src', ['ustc'], type='int', alias='ustc') }},
        {{ trx_json_extract('src', ['ustc_kw'], type='varchar', alias='ustc_kw') }},
        {{ trx_json_extract('src', ['dcur_ref_compnr'], type='int', alias='dcur_ref_compnr') }},
        {{ trx_json_extract('src', ['rtyp_ref_compnr'], type='int', alias='rtyp_ref_compnr') }},
        {{ trx_json_extract('src', ['cpie_ref_compnr'], type='int', alias='cpie_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tcgen000') }}
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
