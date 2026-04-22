{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FAILURES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FAL_LASTSAVED'], type='timestamp_ntz', alias='fal_lastsaved') }},
        {{ trx_json_extract('src', ['FAL_DESC'], type='varchar', alias='fal_desc') }},
        {{ trx_json_extract('src', ['FAL_GEN'], type='varchar', alias='fal_gen') }},
        {{ trx_json_extract('src', ['FAL_UPDATECOUNT'], type='float', alias='fal_updatecount') }},
        {{ trx_json_extract('src', ['FAL_CREATED'], type='timestamp_ntz', alias='fal_created') }},
        {{ trx_json_extract('src', ['FAL_PARTFAILURE'], type='varchar', alias='fal_partfailure') }},
        {{ trx_json_extract('src', ['FAL_NOTUSED'], type='varchar', alias='fal_notused') }},
        {{ trx_json_extract('src', ['FAL_ENABLEWORKORDERS'], type='varchar', alias='fal_enableworkorders') }},
        {{ trx_json_extract('src', ['FAL_GROUP'], type='varchar', alias='fal_group') }},
        {{ trx_json_extract('src', ['FAL_CODE'], type='varchar', alias='fal_code') }},
        {{ trx_json_extract('src', ['FAL_UPDATED'], type='timestamp_ntz', alias='fal_updated') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5failures') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fal_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fal_lastsaved']) }}
