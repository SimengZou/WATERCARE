{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FILTERTABLES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FTA_LASTSAVED'], type='timestamp_ntz', alias='fta_lastsaved') }},
        {{ trx_json_extract('src', ['FTA_TABLE'], type='varchar', alias='fta_table') }},
        {{ trx_json_extract('src', ['FTA_UPDATECOUNT'], type='float', alias='fta_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5filtertables') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fta_table']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fta_lastsaved']) }}
