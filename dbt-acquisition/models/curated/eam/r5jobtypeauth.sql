{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5JOBTYPEAUTH',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['JTA_LASTSAVED'], type='timestamp_ntz', alias='jta_lastsaved') }},
        {{ trx_json_extract('src', ['JTA_JOBTYPE'], type='varchar', alias='jta_jobtype') }},
        {{ trx_json_extract('src', ['JTA_INSERT'], type='varchar', alias='jta_insert') }},
        {{ trx_json_extract('src', ['JTA_UPDATE'], type='varchar', alias='jta_update') }},
        {{ trx_json_extract('src', ['JTA_UPDATECOUNT'], type='float', alias='jta_updatecount') }},
        {{ trx_json_extract('src', ['JTA_UPDATED'], type='timestamp_ntz', alias='jta_updated') }},
        {{ trx_json_extract('src', ['JTA_STATUS'], type='varchar', alias='jta_status') }},
        {{ trx_json_extract('src', ['JTA_GROUP'], type='varchar', alias='jta_group') }},
        {{ trx_json_extract('src', ['JTA_DELETE'], type='varchar', alias='jta_delete') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5jobtypeauth') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['jta_group', 'jta_jobtype', 'jta_status']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['jta_lastsaved']) }}
