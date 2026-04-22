{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5GUMRUPD',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['MRU_EVENT'], type='varchar', alias='mru_event') }},
        {{ trx_json_extract('src', ['MRU_CONTRACTORCODE'], type='varchar', alias='mru_contractorcode') }},
        {{ trx_json_extract('src', ['MRU_UPDATETYPE'], type='varchar', alias='mru_updatetype') }},
        {{ trx_json_extract('src', ['MRU_UPDATEDATE'], type='timestamp_ntz', alias='mru_updatedate') }},
        {{ trx_json_extract('src', ['MRU_COMMENTS'], type='varchar', alias='mru_comments') }},
        {{ trx_json_extract('src', ['MRU_REASON'], type='varchar', alias='mru_reason') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['MRU_TRANSID'], type='varchar', alias='mru_transid') }},
        {{ trx_json_extract('src', ['MRU_NUMBER'], type='varchar', alias='mru_number') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5gumrupd') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mru_transid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
