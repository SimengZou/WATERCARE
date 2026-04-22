{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IMPORTDDFIELDMAP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IDF_LASTSAVED'], type='timestamp_ntz', alias='idf_lastsaved') }},
        {{ trx_json_extract('src', ['IDF_OLD_DDFIELDID'], type='float', alias='idf_old_ddfieldid') }},
        {{ trx_json_extract('src', ['IDF_CREATED'], type='timestamp_ntz', alias='idf_created') }},
        {{ trx_json_extract('src', ['IDF_CREATEDBY'], type='varchar', alias='idf_createdby') }},
        {{ trx_json_extract('src', ['IDF_SESSIONID'], type='float', alias='idf_sessionid') }},
        {{ trx_json_extract('src', ['IDF_DDFIELDID'], type='float', alias='idf_ddfieldid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5importddfieldmap') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['idf_lastsaved']) }}
