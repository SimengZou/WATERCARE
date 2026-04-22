{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5OBJECTSURVEY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OBS_LASTSAVED'], type='timestamp_ntz', alias='obs_lastsaved') }},
        {{ trx_json_extract('src', ['OBS_OBJECT_ORG'], type='varchar', alias='obs_object_org') }},
        {{ trx_json_extract('src', ['OBS_TYPE'], type='varchar', alias='obs_type') }},
        {{ trx_json_extract('src', ['OBS_LEVELPK'], type='varchar', alias='obs_levelpk') }},
        {{ trx_json_extract('src', ['OBS_ANSWERPK'], type='varchar', alias='obs_answerpk') }},
        {{ trx_json_extract('src', ['OBS_UPDATECOUNT'], type='float', alias='obs_updatecount') }},
        {{ trx_json_extract('src', ['OBS_DATEEFFECTIVE'], type='timestamp_ntz', alias='obs_dateeffective') }},
        {{ trx_json_extract('src', ['OBS_CALCULATEDANSWER'], type='varchar', alias='obs_calculatedanswer') }},
        {{ trx_json_extract('src', ['OBS_CALCULATEDVALUE'], type='float', alias='obs_calculatedvalue') }},
        {{ trx_json_extract('src', ['OBS_DATELASTCALCULATED'], type='timestamp_ntz', alias='obs_datelastcalculated') }},
        {{ trx_json_extract('src', ['OBS_WORKORDER'], type='varchar', alias='obs_workorder') }},
        {{ trx_json_extract('src', ['OBS_OPERATORCHECKLIST'], type='varchar', alias='obs_operatorchecklist') }},
        {{ trx_json_extract('src', ['OBS_OBJECT'], type='varchar', alias='obs_object') }},
        {{ trx_json_extract('src', ['OBS_VALUE'], type='float', alias='obs_value') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5objectsurvey') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['obs_object', 'obs_object_org', 'obs_type', 'obs_levelpk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['obs_lastsaved']) }}
