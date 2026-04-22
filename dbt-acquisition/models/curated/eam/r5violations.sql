{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5VIOLATIONS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['VIO_LASTSAVED'], type='timestamp_ntz', alias='vio_lastsaved') }},
        {{ trx_json_extract('src', ['VIO_PASSWORD'], type='varchar', alias='vio_password') }},
        {{ trx_json_extract('src', ['VIO_DATE'], type='timestamp_ntz', alias='vio_date') }},
        {{ trx_json_extract('src', ['VIO_OSMACHINE'], type='varchar', alias='vio_osmachine') }},
        {{ trx_json_extract('src', ['VIO_UPDATECOUNT'], type='float', alias='vio_updatecount') }},
        {{ trx_json_extract('src', ['VIO_USER'], type='varchar', alias='vio_user') }},
        {{ trx_json_extract('src', ['VIO_OSUSER'], type='varchar', alias='vio_osuser') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5violations') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['vio_lastsaved']) }}
