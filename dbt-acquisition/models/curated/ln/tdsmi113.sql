{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tdsmi113 Items by Opportunity table, Generated 2026-04-10T19:41:27Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['opty'], type='varchar', alias='opty') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['seli'], type='int', alias='seli') }},
        {{ trx_json_extract('src', ['seli_kw'], type='varchar', alias='seli_kw') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['qoor'], type='float', alias='qoor') }},
        {{ trx_json_extract('src', ['cuqs'], type='varchar', alias='cuqs') }},
        {{ trx_json_extract('src', ['cvqs'], type='float', alias='cvqs') }},
        {{ trx_json_extract('src', ['amta'], type='float', alias='amta') }},
        {{ trx_json_extract('src', ['prid'], type='varchar', alias='prid') }},
        {{ trx_json_extract('src', ['cprj'], type='varchar', alias='cprj') }},
        {{ trx_json_extract('src', ['cspa'], type='varchar', alias='cspa') }},
        {{ trx_json_extract('src', ['cact'], type='varchar', alias='cact') }},
        {{ trx_json_extract('src', ['opty_ref_compnr'], type='int', alias='opty_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cuqs_ref_compnr'], type='int', alias='cuqs_ref_compnr') }},
        {{ trx_json_extract('src', ['cprj_ref_compnr'], type='int', alias='cprj_ref_compnr') }},
        {{ trx_json_extract('src', ['amta_lclc'], type='float', alias='amta_lclc') }},
        {{ trx_json_extract('src', ['amta_rpc1'], type='float', alias='amta_rpc1') }},
        {{ trx_json_extract('src', ['amta_rpc2'], type='float', alias='amta_rpc2') }},
        {{ trx_json_extract('src', ['amta_rfrc'], type='float', alias='amta_rfrc') }},
        {{ trx_json_extract('src', ['amta_dtwc'], type='float', alias='amta_dtwc') }},
        {{ trx_json_extract('src', ['qoor_invu'], type='float', alias='qoor_invu') }},
        {{ trx_json_extract('src', ['qoor_buar'], type='float', alias='qoor_buar') }},
        {{ trx_json_extract('src', ['qoor_buln'], type='float', alias='qoor_buln') }},
        {{ trx_json_extract('src', ['qoor_bupc'], type='float', alias='qoor_bupc') }},
        {{ trx_json_extract('src', ['qoor_butm'], type='float', alias='qoor_butm') }},
        {{ trx_json_extract('src', ['qoor_buvl'], type='float', alias='qoor_buvl') }},
        {{ trx_json_extract('src', ['qoor_buwg'], type='float', alias='qoor_buwg') }},
        {{ trx_json_extract('src', ['amtg_trnc'], type='float', alias='amtg_trnc') }},
        {{ trx_json_extract('src', ['amtg_lclc'], type='float', alias='amtg_lclc') }},
        {{ trx_json_extract('src', ['amtg_rpc1'], type='float', alias='amtg_rpc1') }},
        {{ trx_json_extract('src', ['amtg_rpc2'], type='float', alias='amtg_rpc2') }},
        {{ trx_json_extract('src', ['amtg_rfrc'], type='float', alias='amtg_rfrc') }},
        {{ trx_json_extract('src', ['amtg_dtwc'], type='float', alias='amtg_dtwc') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tdsmi113') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'opty', 'pono']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
