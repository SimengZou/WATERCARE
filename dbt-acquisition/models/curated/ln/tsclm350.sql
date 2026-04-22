{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tsclm350 Service Resolution - Probability Analysis table, Generated 2026-04-10T19:42:29Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['orig'], type='int', alias='orig') }},
        {{ trx_json_extract('src', ['orig_kw'], type='varchar', alias='orig_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['acln'], type='int', alias='acln') }},
        {{ trx_json_extract('src', ['ccll'], type='varchar', alias='ccll') }},
        {{ trx_json_extract('src', ['hidt'], type='timestamp_ntz', alias='hidt') }},
        {{ trx_json_extract('src', ['sigr'], type='varchar', alias='sigr') }},
        {{ trx_json_extract('src', ['csgr'], type='varchar', alias='csgr') }},
        {{ trx_json_extract('src', ['cgrp'], type='varchar', alias='cgrp') }},
        {{ trx_json_extract('src', ['scgr'], type='varchar', alias='scgr') }},
        {{ trx_json_extract('src', ['ofbp'], type='varchar', alias='ofbp') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['clst'], type='varchar', alias='clst') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['sern'], type='varchar', alias='sern') }},
        {{ trx_json_extract('src', ['rprl'], type='varchar', alias='rprl') }},
        {{ trx_json_extract('src', ['expr'], type='varchar', alias='expr') }},
        {{ trx_json_extract('src', ['espr'], type='varchar', alias='espr') }},
        {{ trx_json_extract('src', ['exsl'], type='varchar', alias='exsl') }},
        {{ trx_json_extract('src', ['sltn'], type='varchar', alias='sltn') }},
        {{ trx_json_extract('src', ['crac'], type='varchar', alias='crac') }},
        {{ trx_json_extract('src', ['sear'], type='varchar', alias='sear') }},
        {{ trx_json_extract('src', ['ccll_ref_compnr'], type='int', alias='ccll_ref_compnr') }},
        {{ trx_json_extract('src', ['sigr_ref_compnr'], type='int', alias='sigr_ref_compnr') }},
        {{ trx_json_extract('src', ['csgr_ref_compnr'], type='int', alias='csgr_ref_compnr') }},
        {{ trx_json_extract('src', ['cgrp_ref_compnr'], type='int', alias='cgrp_ref_compnr') }},
        {{ trx_json_extract('src', ['scgr_ref_compnr'], type='int', alias='scgr_ref_compnr') }},
        {{ trx_json_extract('src', ['ofbp_ref_compnr'], type='int', alias='ofbp_ref_compnr') }},
        {{ trx_json_extract('src', ['clst_ref_compnr'], type='int', alias='clst_ref_compnr') }},
        {{ trx_json_extract('src', ['item_sern_ref_compnr'], type='int', alias='item_sern_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['rprl_ref_compnr'], type='int', alias='rprl_ref_compnr') }},
        {{ trx_json_extract('src', ['expr_ref_compnr'], type='int', alias='expr_ref_compnr') }},
        {{ trx_json_extract('src', ['espr_ref_compnr'], type='int', alias='espr_ref_compnr') }},
        {{ trx_json_extract('src', ['exsl_ref_compnr'], type='int', alias='exsl_ref_compnr') }},
        {{ trx_json_extract('src', ['sltn_ref_compnr'], type='int', alias='sltn_ref_compnr') }},
        {{ trx_json_extract('src', ['crac_ref_compnr'], type='int', alias='crac_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tsclm350') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'orig', 'orno', 'acln', 'ccll', 'hidt']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
