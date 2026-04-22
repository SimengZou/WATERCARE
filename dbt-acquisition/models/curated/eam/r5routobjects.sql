{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ROUTOBJECTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ROB_LASTSAVED'], type='timestamp_ntz', alias='rob_lastsaved') }},
        {{ trx_json_extract('src', ['ROB_OBTYPE'], type='varchar', alias='rob_obtype') }},
        {{ trx_json_extract('src', ['ROB_OBRTYPE'], type='varchar', alias='rob_obrtype') }},
        {{ trx_json_extract('src', ['ROB_OBJECT'], type='varchar', alias='rob_object') }},
        {{ trx_json_extract('src', ['ROB_REVISION'], type='float', alias='rob_revision') }},
        {{ trx_json_extract('src', ['ROB_OBJECT_ORG'], type='varchar', alias='rob_object_org') }},
        {{ trx_json_extract('src', ['ROB_UPDATECOUNT'], type='float', alias='rob_updatecount') }},
        {{ trx_json_extract('src', ['ROB_FROMPOINT'], type='float', alias='rob_frompoint') }},
        {{ trx_json_extract('src', ['ROB_FROMREFDESC'], type='varchar', alias='rob_fromrefdesc') }},
        {{ trx_json_extract('src', ['ROB_FROMGEOREF'], type='varchar', alias='rob_fromgeoref') }},
        {{ trx_json_extract('src', ['ROB_TOPOINT'], type='float', alias='rob_topoint') }},
        {{ trx_json_extract('src', ['ROB_TOREFDESC'], type='varchar', alias='rob_torefdesc') }},
        {{ trx_json_extract('src', ['ROB_TOGEOREF'], type='varchar', alias='rob_togeoref') }},
        {{ trx_json_extract('src', ['ROB_FROM_REFERENCE'], type='float', alias='rob_from_reference') }},
        {{ trx_json_extract('src', ['ROB_FROM_OFFSET'], type='float', alias='rob_from_offset') }},
        {{ trx_json_extract('src', ['ROB_FROM_OFFSET_DIRECTION'], type='varchar', alias='rob_from_offset_direction') }},
        {{ trx_json_extract('src', ['ROB_TO_REFERENCE'], type='float', alias='rob_to_reference') }},
        {{ trx_json_extract('src', ['ROB_TO_OFFSET'], type='float', alias='rob_to_offset') }},
        {{ trx_json_extract('src', ['ROB_TO_OFFSET_DIRECTION'], type='varchar', alias='rob_to_offset_direction') }},
        {{ trx_json_extract('src', ['ROB_ROUTE'], type='varchar', alias='rob_route') }},
        {{ trx_json_extract('src', ['ROB_LINE'], type='float', alias='rob_line') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5routobjects') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rob_route', 'rob_revision', 'rob_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rob_lastsaved']) }}
