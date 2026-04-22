{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whwmd217 Item Inventory by Warehouse Cost Details table, Generated 2025-06-12T01:48:45Z from package combination ce01090',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['cpcp'], type='varchar', alias='cpcp') }},
        {{ trx_json_extract('src', ['ftph'], type='float', alias='ftph') }},
        {{ trx_json_extract('src', ['mauh'], type='float', alias='mauh') }},
        {{ trx_json_extract('src', ['ftpa_1'], type='float', alias='ftpa_1') }},
        {{ trx_json_extract('src', ['ftpa_2'], type='float', alias='ftpa_2') }},
        {{ trx_json_extract('src', ['ftpa_3'], type='float', alias='ftpa_3') }},
        {{ trx_json_extract('src', ['mauc_1'], type='float', alias='mauc_1') }},
        {{ trx_json_extract('src', ['mauc_2'], type='float', alias='mauc_2') }},
        {{ trx_json_extract('src', ['mauc_3'], type='float', alias='mauc_3') }},
        {{ trx_json_extract('src', ['cwar_item_ref_compnr'], type='int', alias='cwar_item_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_ref_compnr'], type='int', alias='cwar_ref_compnr') }},
        {{ trx_json_extract('src', ['item_ref_compnr'], type='int', alias='item_ref_compnr') }},
        {{ trx_json_extract('src', ['cpcp_ref_compnr'], type='int', alias='cpcp_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whwmd217') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'cwar', 'item', 'cpcp']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
