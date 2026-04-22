{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whwmd215 Item Inventory by Warehouse table, Generated 2026-04-10T19:42:53Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['qhnd'], type='float', alias='qhnd') }},
        {{ trx_json_extract('src', ['qchd'], type='float', alias='qchd') }},
        {{ trx_json_extract('src', ['qnhd'], type='float', alias='qnhd') }},
        {{ trx_json_extract('src', ['qblk'], type='float', alias='qblk') }},
        {{ trx_json_extract('src', ['qbpl'], type='float', alias='qbpl') }},
        {{ trx_json_extract('src', ['qord'], type='float', alias='qord') }},
        {{ trx_json_extract('src', ['qoor'], type='float', alias='qoor') }},
        {{ trx_json_extract('src', ['qcor'], type='float', alias='qcor') }},
        {{ trx_json_extract('src', ['qnor'], type='float', alias='qnor') }},
        {{ trx_json_extract('src', ['qgit'], type='float', alias='qgit') }},
        {{ trx_json_extract('src', ['qint'], type='float', alias='qint') }},
        {{ trx_json_extract('src', ['qcit'], type='float', alias='qcit') }},
        {{ trx_json_extract('src', ['qnit'], type='float', alias='qnit') }},
        {{ trx_json_extract('src', ['qall'], type='float', alias='qall') }},
        {{ trx_json_extract('src', ['qoal'], type='float', alias='qoal') }},
        {{ trx_json_extract('src', ['qcal'], type='float', alias='qcal') }},
        {{ trx_json_extract('src', ['qnal'], type='float', alias='qnal') }},
        {{ trx_json_extract('src', ['qnbl'], type='float', alias='qnbl') }},
        {{ trx_json_extract('src', ['qnbp'], type='float', alias='qnbp') }},
        {{ trx_json_extract('src', ['qcom'], type='float', alias='qcom') }},
        {{ trx_json_extract('src', ['qlal'], type='float', alias='qlal') }},
        {{ trx_json_extract('src', ['qcpr'], type='float', alias='qcpr') }},
        {{ trx_json_extract('src', ['qhrj'], type='float', alias='qhrj') }},
        {{ trx_json_extract('src', ['qcrj'], type='float', alias='qcrj') }},
        {{ trx_json_extract('src', ['qnrj'], type='float', alias='qnrj') }},
        {{ trx_json_extract('src', ['ltdt'], type='timestamp_ntz', alias='ltdt') }},
        {{ trx_json_extract('src', ['hstd'], type='timestamp_ntz', alias='hstd') }},
        {{ trx_json_extract('src', ['lsid'], type='timestamp_ntz', alias='lsid') }},
        {{ trx_json_extract('src', ['qcis'], type='float', alias='qcis') }},
        {{ trx_json_extract('src', ['qhib'], type='float', alias='qhib') }},
        {{ trx_json_extract('src', ['cwar_item_ref_compnr'], type='int', alias='cwar_item_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whwmd215') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cwar', 'item']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
