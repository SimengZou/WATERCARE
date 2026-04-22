{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWMETASTATUSES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DMS_LASTSAVED'], type='timestamp_ntz', alias='dms_lastsaved') }},
        {{ trx_json_extract('src', ['DMS_FIELD'], type='varchar', alias='dms_field') }},
        {{ trx_json_extract('src', ['DMS_STATUSENTITY'], type='varchar', alias='dms_statusentity') }},
        {{ trx_json_extract('src', ['DMS_STATUSENTITYBOT'], type='float', alias='dms_statusentitybot') }},
        {{ trx_json_extract('src', ['DMS_ENTITYBOT'], type='float', alias='dms_entitybot') }},
        {{ trx_json_extract('src', ['DMS_UPDATED'], type='timestamp_ntz', alias='dms_updated') }},
        {{ trx_json_extract('src', ['DMS_TABLE'], type='varchar', alias='dms_table') }},
        {{ trx_json_extract('src', ['DMS_ENTITY'], type='varchar', alias='dms_entity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwmetastatuses') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dms_lastsaved']) }}
