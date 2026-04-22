{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsctm320 Contract Changes table, Generated 2026-04-10T19:42:33Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['csco'], type='varchar', alias='csco') }},
        {{ trx_json_extract('src', ['cchn'], type='int', alias='cchn') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['actn'], type='int', alias='actn') }},
        {{ trx_json_extract('src', ['actn_kw'], type='varchar', alias='actn_kw') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['term'], type='int', alias='term') }},
        {{ trx_json_extract('src', ['nrpe'], type='int', alias='nrpe') }},
        {{ trx_json_extract('src', ['peru'], type='int', alias='peru') }},
        {{ trx_json_extract('src', ['peru_kw'], type='varchar', alias='peru_kw') }},
        {{ trx_json_extract('src', ['ochn'], type='int', alias='ochn') }},
        {{ trx_json_extract('src', ['icpn'], type='float', alias='icpn') }},
        {{ trx_json_extract('src', ['efdt'], type='date', alias='efdt') }},
        {{ trx_json_extract('src', ['exdt'], type='date', alias='exdt') }},
        {{ trx_json_extract('src', ['chdt'], type='date', alias='chdt') }},
        {{ trx_json_extract('src', ['cind'], type='varchar', alias='cind') }},
        {{ trx_json_extract('src', ['crtm'], type='timestamp_ntz', alias='crtm') }},
        {{ trx_json_extract('src', ['csmt'], type='float', alias='csmt') }},
        {{ trx_json_extract('src', ['rsmt'], type='float', alias='rsmt') }},
        {{ trx_json_extract('src', ['camt_1'], type='float', alias='camt_1') }},
        {{ trx_json_extract('src', ['camt_2'], type='float', alias='camt_2') }},
        {{ trx_json_extract('src', ['camt_3'], type='float', alias='camt_3') }},
        {{ trx_json_extract('src', ['erfa'], type='float', alias='erfa') }},
        {{ trx_json_extract('src', ['amdy'], type='float', alias='amdy') }},
        {{ trx_json_extract('src', ['cody_1'], type='float', alias='cody_1') }},
        {{ trx_json_extract('src', ['cody_2'], type='float', alias='cody_2') }},
        {{ trx_json_extract('src', ['cody_3'], type='float', alias='cody_3') }},
        {{ trx_json_extract('src', ['inpc'], type='float', alias='inpc') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['csco_ref_compnr'], type='int', alias='csco_ref_compnr') }},
        {{ trx_json_extract('src', ['cind_ref_compnr'], type='int', alias='cind_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['rsmt_homc'], type='float', alias='rsmt_homc') }},
        {{ trx_json_extract('src', ['rsmt_rpc1'], type='float', alias='rsmt_rpc1') }},
        {{ trx_json_extract('src', ['rsmt_rpc2'], type='float', alias='rsmt_rpc2') }},
        {{ trx_json_extract('src', ['rsmt_refc'], type='float', alias='rsmt_refc') }},
        {{ trx_json_extract('src', ['rsmt_dwhc'], type='float', alias='rsmt_dwhc') }},
        {{ trx_json_extract('src', ['camt_cntc'], type='float', alias='camt_cntc') }},
        {{ trx_json_extract('src', ['camt_refc'], type='float', alias='camt_refc') }},
        {{ trx_json_extract('src', ['camt_dwhc'], type='float', alias='camt_dwhc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsctm320') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'csco', 'cchn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
