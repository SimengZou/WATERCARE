{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5UCODES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UCO_LASTSAVED'], type='timestamp_ntz', alias='uco_lastsaved') }},
        {{ trx_json_extract('src', ['UCO_IMAGE'], type='varchar', alias='uco_image') }},
        {{ trx_json_extract('src', ['UCO_CODE'], type='varchar', alias='uco_code') }},
        {{ trx_json_extract('src', ['UCO_RCODE'], type='varchar', alias='uco_rcode') }},
        {{ trx_json_extract('src', ['UCO_SYSTEM'], type='varchar', alias='uco_system') }},
        {{ trx_json_extract('src', ['UCO_DESC'], type='varchar', alias='uco_desc') }},
        {{ trx_json_extract('src', ['UCO_UPDATECOUNT'], type='float', alias='uco_updatecount') }},
        {{ trx_json_extract('src', ['UCO_CREATED'], type='timestamp_ntz', alias='uco_created') }},
        {{ trx_json_extract('src', ['UCO_UPDATED'], type='timestamp_ntz', alias='uco_updated') }},
        {{ trx_json_extract('src', ['UCO_TEXTCODE'], type='varchar', alias='uco_textcode') }},
        {{ trx_json_extract('src', ['UCO_NOTUSED'], type='varchar', alias='uco_notused') }},
        {{ trx_json_extract('src', ['UCO_ICON'], type='varchar', alias='uco_icon') }},
        {{ trx_json_extract('src', ['UCO_ICONPATH'], type='varchar', alias='uco_iconpath') }},
        {{ trx_json_extract('src', ['UCO_WEIGHT'], type='float', alias='uco_weight') }},
        {{ trx_json_extract('src', ['UCO_COLOR'], type='varchar', alias='uco_color') }},
        {{ trx_json_extract('src', ['UCO_CATEGORY'], type='varchar', alias='uco_category') }},
        {{ trx_json_extract('src', ['UCO_GISDISPATCHRANKING'], type='float', alias='uco_gisdispatchranking') }},
        {{ trx_json_extract('src', ['UCO_CUADJUSTMENT'], type='float', alias='uco_cuadjustment') }},
        {{ trx_json_extract('src', ['UCO_ENTITY'], type='varchar', alias='uco_entity') }},
        {{ trx_json_extract('src', ['UCO_RENTITY'], type='varchar', alias='uco_rentity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5ucodes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['uco_entity', 'uco_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['uco_lastsaved']) }}
