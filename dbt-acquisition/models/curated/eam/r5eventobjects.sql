{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EVENTOBJECTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EOB_LASTSAVED'], type='timestamp_ntz', alias='eob_lastsaved') }},
        {{ trx_json_extract('src', ['EOB_OBTYPE'], type='varchar', alias='eob_obtype') }},
        {{ trx_json_extract('src', ['EOB_OBRTYPE'], type='varchar', alias='eob_obrtype') }},
        {{ trx_json_extract('src', ['EOB_OBJECT'], type='varchar', alias='eob_object') }},
        {{ trx_json_extract('src', ['EOB_LEVEL'], type='float', alias='eob_level') }},
        {{ trx_json_extract('src', ['EOB_ROLLUP'], type='varchar', alias='eob_rollup') }},
        {{ trx_json_extract('src', ['EOB_OBJECT_ORG'], type='varchar', alias='eob_object_org') }},
        {{ trx_json_extract('src', ['EOB_UPDATECOUNT'], type='float', alias='eob_updatecount') }},
        {{ trx_json_extract('src', ['EOB_FROMPOINT'], type='float', alias='eob_frompoint') }},
        {{ trx_json_extract('src', ['EOB_TOPOINT'], type='float', alias='eob_topoint') }},
        {{ trx_json_extract('src', ['EOB_WEIGHTPERCENTAGE'], type='float', alias='eob_weightpercentage') }},
        {{ trx_json_extract('src', ['EOB_EVENT'], type='varchar', alias='eob_event') }},
        {{ trx_json_extract('src', ['EOB_DOWNTIME'], type='varchar', alias='eob_downtime') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5eventobjects') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['eob_event', 'eob_object', 'eob_object_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['eob_lastsaved']) }}
