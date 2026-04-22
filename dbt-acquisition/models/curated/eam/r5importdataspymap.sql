{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IMPORTDATASPYMAP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IDS_LASTSAVED'], type='timestamp_ntz', alias='ids_lastsaved') }},
        {{ trx_json_extract('src', ['IDS_OLD_DATASPYID'], type='float', alias='ids_old_dataspyid') }},
        {{ trx_json_extract('src', ['IDS_CREATED'], type='timestamp_ntz', alias='ids_created') }},
        {{ trx_json_extract('src', ['IDS_CREATEDBY'], type='varchar', alias='ids_createdby') }},
        {{ trx_json_extract('src', ['IDS_SESSIONID'], type='float', alias='ids_sessionid') }},
        {{ trx_json_extract('src', ['IDS_DATASPYID'], type='float', alias='ids_dataspyid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5importdataspymap') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ids_lastsaved']) }}
