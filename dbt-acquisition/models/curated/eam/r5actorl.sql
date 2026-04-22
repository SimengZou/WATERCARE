{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ACTORL',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ACT_LASTSAVED'], type='timestamp_ntz', alias='act_lastsaved') }},
        {{ trx_json_extract('src', ['ACT_ACT'], type='float', alias='act_act') }},
        {{ trx_json_extract('src', ['EVT_ORG'], type='varchar', alias='evt_org') }},
        {{ trx_json_extract('src', ['ACT_ORDER_ORG'], type='varchar', alias='act_order_org') }},
        {{ trx_json_extract('src', ['ACT_ORDLINE'], type='float', alias='act_ordline') }},
        {{ trx_json_extract('src', ['ACT_EVENT'], type='varchar', alias='act_event') }},
        {{ trx_json_extract('src', ['ACT_ORDER'], type='varchar', alias='act_order') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5actorl') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['act_lastsaved']) }}
