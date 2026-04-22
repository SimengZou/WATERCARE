{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5JOBS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['JOB_LASTSAVED'], type='timestamp_ntz', alias='job_lastsaved') }},
        {{ trx_json_extract('src', ['JOB_DESCRIPTION'], type='varchar', alias='job_description') }},
        {{ trx_json_extract('src', ['JOB_CLASS'], type='varchar', alias='job_class') }},
        {{ trx_json_extract('src', ['JOB_UPDATECOUNT'], type='float', alias='job_updatecount') }},
        {{ trx_json_extract('src', ['JOB_PARTNERID'], type='varchar', alias='job_partnerid') }},
        {{ trx_json_extract('src', ['JOB_NAME'], type='varchar', alias='job_name') }},
        {{ trx_json_extract('src', ['JOB_TYPE'], type='varchar', alias='job_type') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5jobs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['job_name']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['job_lastsaved']) }}
