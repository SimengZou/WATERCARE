{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FUNNOPERM',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FPN_LASTSAVED'], type='timestamp_ntz', alias='fpn_lastsaved') }},
        {{ trx_json_extract('src', ['FPN_NOSELECT'], type='varchar', alias='fpn_noselect') }},
        {{ trx_json_extract('src', ['FPN_NOINSERT'], type='varchar', alias='fpn_noinsert') }},
        {{ trx_json_extract('src', ['FPN_NOUPDATE'], type='varchar', alias='fpn_noupdate') }},
        {{ trx_json_extract('src', ['FPN_UPDATECOUNT'], type='float', alias='fpn_updatecount') }},
        {{ trx_json_extract('src', ['FPN_FUNCTION'], type='varchar', alias='fpn_function') }},
        {{ trx_json_extract('src', ['FPN_NODELETE'], type='varchar', alias='fpn_nodelete') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5funnoperm') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fpn_function']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fpn_lastsaved']) }}
