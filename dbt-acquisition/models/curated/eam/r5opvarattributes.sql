{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5OPVARATTRIBUTES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OAT_LASTSAVED'], type='timestamp_ntz', alias='oat_lastsaved') }},
        {{ trx_json_extract('src', ['OAT_DESC'], type='varchar', alias='oat_desc') }},
        {{ trx_json_extract('src', ['OAT_TYPE'], type='float', alias='oat_type') }},
        {{ trx_json_extract('src', ['OAT_AUDITUSER'], type='varchar', alias='oat_audituser') }},
        {{ trx_json_extract('src', ['OAT_AUDITTIMESTAMP'], type='timestamp_ntz', alias='oat_audittimestamp') }},
        {{ trx_json_extract('src', ['OAT_UPDATECOUNT'], type='float', alias='oat_updatecount') }},
        {{ trx_json_extract('src', ['OAT_ID'], type='float', alias='oat_id') }},
        {{ trx_json_extract('src', ['OAT_LABEL'], type='varchar', alias='oat_label') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5opvarattributes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['oat_lastsaved']) }}
