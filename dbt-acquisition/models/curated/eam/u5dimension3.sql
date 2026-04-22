{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5DIMENSION3',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['DESCRIPTION'], type='varchar', alias='description') }},
        {{ trx_json_extract('src', ['DIM3_NOTUSED'], type='varchar', alias='dim3_notused') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['DIM3_CODE'], type='varchar', alias='dim3_code') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5dimension3') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dim3_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
