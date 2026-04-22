{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5ASSETCLASS_ACTIVITY',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['ACAA_TASKTYPE'], type='varchar', alias='acaa_tasktype') }},
        {{ trx_json_extract('src', ['ACAA_NOTUSED'], type='varchar', alias='acaa_notused') }},
        {{ trx_json_extract('src', ['ACAA_ASSETCLASS_DESC'], type='varchar', alias='acaa_assetclass_desc') }},
        {{ trx_json_extract('src', ['ACAA_TASKTYPE_DESC'], type='varchar', alias='acaa_tasktype_desc') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['ACAA_ASSETCLASS'], type='varchar', alias='acaa_assetclass') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5assetclass_activity') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['acaa_assetclass', 'acaa_tasktype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
