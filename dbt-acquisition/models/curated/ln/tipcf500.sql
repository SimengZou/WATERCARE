{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tipcf500 Product Variant IDs table, Generated 2026-04-10T19:41:47Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cpva'], type='int', alias='cpva') }},
        {{ trx_json_extract('src', ['cuid', 'en_US'], type='varchar', alias='cuid__en_us') }},
        {{ trx_json_extract('src', ['dsca', 'en_US'], type='varchar', alias='dsca__en_us') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['reft'], type='int', alias='reft') }},
        {{ trx_json_extract('src', ['reft_kw'], type='varchar', alias='reft_kw') }},
        {{ trx_json_extract('src', ['refo'], type='varchar', alias='refo') }},
        {{ trx_json_extract('src', ['refp'], type='int', alias='refp') }},
        {{ trx_json_extract('src', ['altn'], type='int', alias='altn') }},
        {{ trx_json_extract('src', ['sost'], type='int', alias='sost') }},
        {{ trx_json_extract('src', ['sost_kw'], type='varchar', alias='sost_kw') }},
        {{ trx_json_extract('src', ['cuno'], type='varchar', alias='cuno') }},
        {{ trx_json_extract('src', ['ccur'], type='varchar', alias='ccur') }},
        {{ trx_json_extract('src', ['slpr'], type='float', alias='slpr') }},
        {{ trx_json_extract('src', ['lnst'], type='int', alias='lnst') }},
        {{ trx_json_extract('src', ['lnst_kw'], type='varchar', alias='lnst_kw') }},
        {{ trx_json_extract('src', ['vali'], type='int', alias='vali') }},
        {{ trx_json_extract('src', ['vali_kw'], type='varchar', alias='vali_kw') }},
        {{ trx_json_extract('src', ['pcfd'], type='timestamp_ntz', alias='pcfd') }},
        {{ trx_json_extract('src', ['olid'], type='varchar', alias='olid') }},
        {{ trx_json_extract('src', ['acfv'], type='int', alias='acfv') }},
        {{ trx_json_extract('src', ['acfv_kw'], type='varchar', alias='acfv_kw') }},
        {{ trx_json_extract('src', ['acfs'], type='int', alias='acfs') }},
        {{ trx_json_extract('src', ['acfs_kw'], type='varchar', alias='acfs_kw') }},
        {{ trx_json_extract('src', ['spcf'], type='int', alias='spcf') }},
        {{ trx_json_extract('src', ['spcf_kw'], type='varchar', alias='spcf_kw') }},
        {{ trx_json_extract('src', ['imag'], type='varchar', alias='imag') }},
        {{ trx_json_extract('src', ['irft'], type='int', alias='irft') }},
        {{ trx_json_extract('src', ['irft_kw'], type='varchar', alias='irft_kw') }},
        {{ trx_json_extract('src', ['irfo'], type='varchar', alias='irfo') }},
        {{ trx_json_extract('src', ['enho'], type='int', alias='enho') }},
        {{ trx_json_extract('src', ['enho_kw'], type='varchar', alias='enho_kw') }},
        {{ trx_json_extract('src', ['qana'], type='float', alias='qana') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['site'], type='varchar', alias='site') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['ccty'], type='varchar', alias='ccty') }},
        {{ trx_json_extract('src', ['prds'], type='varchar', alias='prds') }},
        {{ trx_json_extract('src', ['alws', 'en_US'], type='varchar', alias='alws__en_us') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cuno_ref_compnr'], type='int', alias='cuno_ref_compnr') }},
        {{ trx_json_extract('src', ['ccur_ref_compnr'], type='int', alias='ccur_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tipcf500') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cpva']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
