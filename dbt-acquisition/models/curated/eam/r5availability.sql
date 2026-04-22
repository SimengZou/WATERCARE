{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5AVAILABILITY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AVL_LASTSAVED'], type='timestamp_ntz', alias='avl_lastsaved') }},
        {{ trx_json_extract('src', ['AVL_NETHOURS'], type='float', alias='avl_nethours') }},
        {{ trx_json_extract('src', ['AVL_HIRHOURS'], type='float', alias='avl_hirhours') }},
        {{ trx_json_extract('src', ['AVL_NETHIRED'], type='float', alias='avl_nethired') }},
        {{ trx_json_extract('src', ['AVL_DATE'], type='timestamp_ntz', alias='avl_date') }},
        {{ trx_json_extract('src', ['AVL_MRC'], type='varchar', alias='avl_mrc') }},
        {{ trx_json_extract('src', ['AVL_TRADE'], type='varchar', alias='avl_trade') }},
        {{ trx_json_extract('src', ['AVL_UPDATECOUNT'], type='float', alias='avl_updatecount') }},
        {{ trx_json_extract('src', ['AVL_OWNHOURS'], type='float', alias='avl_ownhours') }},
        {{ trx_json_extract('src', ['AVL_SPECIAL'], type='varchar', alias='avl_special') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5availability') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['avl_date', 'avl_mrc', 'avl_trade']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['avl_lastsaved']) }}
