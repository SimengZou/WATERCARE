{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CAUSES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CAU_LASTSAVED'], type='timestamp_ntz', alias='cau_lastsaved') }},
        {{ trx_json_extract('src', ['CAU_DESC'], type='varchar', alias='cau_desc') }},
        {{ trx_json_extract('src', ['CAU_GEN'], type='varchar', alias='cau_gen') }},
        {{ trx_json_extract('src', ['CAU_UPDATECOUNT'], type='float', alias='cau_updatecount') }},
        {{ trx_json_extract('src', ['CAU_CREATED'], type='timestamp_ntz', alias='cau_created') }},
        {{ trx_json_extract('src', ['CAU_PARTFAILURE'], type='varchar', alias='cau_partfailure') }},
        {{ trx_json_extract('src', ['CAU_NOTUSED'], type='varchar', alias='cau_notused') }},
        {{ trx_json_extract('src', ['CAU_ENABLEWORKORDERS'], type='varchar', alias='cau_enableworkorders') }},
        {{ trx_json_extract('src', ['CAU_GROUP'], type='varchar', alias='cau_group') }},
        {{ trx_json_extract('src', ['CAU_CODE'], type='varchar', alias='cau_code') }},
        {{ trx_json_extract('src', ['CAU_UPDATED'], type='timestamp_ntz', alias='cau_updated') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5causes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cau_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cau_lastsaved']) }}
