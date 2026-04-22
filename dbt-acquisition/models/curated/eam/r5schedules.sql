{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SCHEDULES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SCH_LASTSAVED'], type='timestamp_ntz', alias='sch_lastsaved') }},
        {{ trx_json_extract('src', ['SCH_NAME'], type='varchar', alias='sch_name') }},
        {{ trx_json_extract('src', ['SCH_DESCRIPTION'], type='varchar', alias='sch_description') }},
        {{ trx_json_extract('src', ['SCH_TYPE'], type='varchar', alias='sch_type') }},
        {{ trx_json_extract('src', ['SCH_MONTH'], type='varchar', alias='sch_month') }},
        {{ trx_json_extract('src', ['SCH_DAYOFWEEK'], type='varchar', alias='sch_dayofweek') }},
        {{ trx_json_extract('src', ['SCH_HOUR'], type='varchar', alias='sch_hour') }},
        {{ trx_json_extract('src', ['SCH_MINUTE'], type='varchar', alias='sch_minute') }},
        {{ trx_json_extract('src', ['SCH_UPDATECOUNT'], type='float', alias='sch_updatecount') }},
        {{ trx_json_extract('src', ['SCH_CODE'], type='varchar', alias='sch_code') }},
        {{ trx_json_extract('src', ['SCH_DAYOFMONTH'], type='varchar', alias='sch_dayofmonth') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5schedules') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['sch_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sch_lastsaved']) }}
