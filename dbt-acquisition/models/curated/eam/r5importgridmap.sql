{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IMPORTGRIDMAP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IGM_LASTSAVED'], type='timestamp_ntz', alias='igm_lastsaved') }},
        {{ trx_json_extract('src', ['IGM_OLD_GRIDID'], type='float', alias='igm_old_gridid') }},
        {{ trx_json_extract('src', ['IGM_GRIDID'], type='float', alias='igm_gridid') }},
        {{ trx_json_extract('src', ['IGM_CREATED'], type='timestamp_ntz', alias='igm_created') }},
        {{ trx_json_extract('src', ['IGM_CREATEDBY'], type='varchar', alias='igm_createdby') }},
        {{ trx_json_extract('src', ['IGM_SESSIONID'], type='float', alias='igm_sessionid') }},
        {{ trx_json_extract('src', ['IGM_ACTIVE'], type='varchar', alias='igm_active') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5importgridmap') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['igm_lastsaved']) }}
