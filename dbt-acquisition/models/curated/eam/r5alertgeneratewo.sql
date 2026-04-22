{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTGENERATEWO',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AGW_LASTSAVED'], type='timestamp_ntz', alias='agw_lastsaved') }},
        {{ trx_json_extract('src', ['AGW_TYPE'], type='varchar', alias='agw_type') }},
        {{ trx_json_extract('src', ['AGW_UPDATECOUNT'], type='float', alias='agw_updatecount') }},
        {{ trx_json_extract('src', ['AGW_ALERT'], type='varchar', alias='agw_alert') }},
        {{ trx_json_extract('src', ['AGW_GENERATETHROUGHFIELDID'], type='float', alias='agw_generatethroughfieldid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alertgeneratewo') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['agw_alert']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['agw_lastsaved']) }}
