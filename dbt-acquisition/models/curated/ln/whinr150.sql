{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN whinr150 Inventory Structure table, Generated 2025-06-12T01:48:44Z from package combination ce01090',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['item'], type='varchar', alias='item') }},
        {{ trx_json_extract('src', ['cwar'], type='varchar', alias='cwar') }},
        {{ trx_json_extract('src', ['loca'], type='varchar', alias='loca') }},
        {{ trx_json_extract('src', ['clot'], type='varchar', alias='clot') }},
        {{ trx_json_extract('src', ['idat'], type='timestamp_ntz', alias='idat') }},
        {{ trx_json_extract('src', ['pkdf'], type='varchar', alias='pkdf') }},
        {{ trx_json_extract('src', ['levl'], type='int', alias='levl') }},
        {{ trx_json_extract('src', ['cuni'], type='varchar', alias='cuni') }},
        {{ trx_json_extract('src', ['qhds'], type='float', alias='qhds') }},
        {{ trx_json_extract('src', ['qhnd'], type='float', alias='qhnd') }},
        {{ trx_json_extract('src', ['prbl'], type='float', alias='prbl') }},
        {{ trx_json_extract('src', ['mabl'], type='float', alias='mabl') }},
        {{ trx_json_extract('src', ['mabp'], type='float', alias='mabp') }},
        {{ trx_json_extract('src', ['qals'], type='float', alias='qals') }},
        {{ trx_json_extract('src', ['qcom'], type='float', alias='qcom') }},
        {{ trx_json_extract('src', ['qcsl'], type='float', alias='qcsl') }},
        {{ trx_json_extract('src', ['qavs'], type='float', alias='qavs') }},
        {{ trx_json_extract('src', ['qavl'], type='float', alias='qavl') }},
        {{ trx_json_extract('src', ['item_pkdf_levl_ref_compnr'], type='int', alias='item_pkdf_levl_ref_compnr') }},
        {{ trx_json_extract('src', ['cwar_loca_item_clot_idat_ref_compnr'], type='int', alias='cwar_loca_item_clot_idat_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_whinr150') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'item', 'cwar', 'loca', 'clot', 'idat', 'pkdf', 'levl', 'cuni']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
