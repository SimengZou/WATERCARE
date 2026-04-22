{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SCHEDULEDJOBS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SCJ_LASTSAVED'], type='timestamp_ntz', alias='scj_lastsaved') }},
        {{ trx_json_extract('src', ['SCJ_JOBNAME'], type='varchar', alias='scj_jobname') }},
        {{ trx_json_extract('src', ['SCJ_SCHCODE'], type='varchar', alias='scj_schcode') }},
        {{ trx_json_extract('src', ['SCJ_ACTIVE'], type='varchar', alias='scj_active') }},
        {{ trx_json_extract('src', ['SCJ_BROKEN'], type='varchar', alias='scj_broken') }},
        {{ trx_json_extract('src', ['SCJ_NEXTRUN'], type='timestamp_ntz', alias='scj_nextrun') }},
        {{ trx_json_extract('src', ['SCJ_UPDATECOUNT'], type='float', alias='scj_updatecount') }},
        {{ trx_json_extract('src', ['SCJ_SERVERID'], type='varchar', alias='scj_serverid') }},
        {{ trx_json_extract('src', ['SCJ_TYPE'], type='varchar', alias='scj_type') }},
        {{ trx_json_extract('src', ['SCJ_CODE'], type='varchar', alias='scj_code') }},
        {{ trx_json_extract('src', ['SCJ_LASTRUN'], type='timestamp_ntz', alias='scj_lastrun') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5scheduledjobs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['scj_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['scj_lastsaved']) }}
