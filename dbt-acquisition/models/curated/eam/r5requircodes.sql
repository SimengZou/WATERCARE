{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REQUIRCODES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RQM_LASTSAVED'], type='timestamp_ntz', alias='rqm_lastsaved') }},
        {{ trx_json_extract('src', ['RQM_DESC'], type='varchar', alias='rqm_desc') }},
        {{ trx_json_extract('src', ['RQM_CLASS'], type='varchar', alias='rqm_class') }},
        {{ trx_json_extract('src', ['RQM_GEN'], type='varchar', alias='rqm_gen') }},
        {{ trx_json_extract('src', ['RQM_CLASS_ORG'], type='varchar', alias='rqm_class_org') }},
        {{ trx_json_extract('src', ['RQM_UPDATECOUNT'], type='float', alias='rqm_updatecount') }},
        {{ trx_json_extract('src', ['RQM_UPDATED'], type='timestamp_ntz', alias='rqm_updated') }},
        {{ trx_json_extract('src', ['RQM_PARTFAILURE'], type='varchar', alias='rqm_partfailure') }},
        {{ trx_json_extract('src', ['RQM_NOTUSED'], type='varchar', alias='rqm_notused') }},
        {{ trx_json_extract('src', ['RQM_ENABLEWORKORDERS'], type='varchar', alias='rqm_enableworkorders') }},
        {{ trx_json_extract('src', ['RQM_GROUP'], type='varchar', alias='rqm_group') }},
        {{ trx_json_extract('src', ['RQM_CODE'], type='varchar', alias='rqm_code') }},
        {{ trx_json_extract('src', ['RQM_CREATED'], type='timestamp_ntz', alias='rqm_created') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5requircodes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rqm_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rqm_lastsaved']) }}
