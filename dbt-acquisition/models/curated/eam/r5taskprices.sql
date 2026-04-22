{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TASKPRICES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TPR_LASTSAVED'], type='timestamp_ntz', alias='tpr_lastsaved') }},
        {{ trx_json_extract('src', ['TPR_ORG'], type='varchar', alias='tpr_org') }},
        {{ trx_json_extract('src', ['TPR_TASK'], type='varchar', alias='tpr_task') }},
        {{ trx_json_extract('src', ['TPR_UPDATECOUNT'], type='float', alias='tpr_updatecount') }},
        {{ trx_json_extract('src', ['TPR_REVISION'], type='float', alias='tpr_revision') }},
        {{ trx_json_extract('src', ['TPR_PRICE'], type='float', alias='tpr_price') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5taskprices') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['tpr_task', 'tpr_revision', 'tpr_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['tpr_lastsaved']) }}
