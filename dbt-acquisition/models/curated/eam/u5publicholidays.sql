{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5PUBLICHOLIDAYS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['HLY_ORG'], type='varchar', alias='hly_org') }},
        {{ trx_json_extract('src', ['HLY_CONCODE'], type='varchar', alias='hly_concode') }},
        {{ trx_json_extract('src', ['HLY_YEAR'], type='varchar', alias='hly_year') }},
        {{ trx_json_extract('src', ['HLY_DATE'], type='timestamp_ntz', alias='hly_date') }},
        {{ trx_json_extract('src', ['HLY_NAME'], type='varchar', alias='hly_name') }},
        {{ trx_json_extract('src', ['AUTO'], type='float', alias='auto') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['HLY_ID'], type='varchar', alias='hly_id') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5publicholidays') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['hly_id']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
