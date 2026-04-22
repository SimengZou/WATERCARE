{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CUSTOM',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CUS_LASTSAVED'], type='timestamp_ntz', alias='cus_lastsaved') }},
        {{ trx_json_extract('src', ['CUS_APPLIED'], type='timestamp_ntz', alias='cus_applied') }},
        {{ trx_json_extract('src', ['CUS_TITLE'], type='varchar', alias='cus_title') }},
        {{ trx_json_extract('src', ['CUS_DESC'], type='varchar', alias='cus_desc') }},
        {{ trx_json_extract('src', ['CUS_UPDATECOUNT'], type='float', alias='cus_updatecount') }},
        {{ trx_json_extract('src', ['CUS_DATE'], type='timestamp_ntz', alias='cus_date') }},
        {{ trx_json_extract('src', ['CUS_OBJECTS'], type='varchar', alias='cus_objects') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5custom') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cus_date', 'cus_title', 'cus_objects']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cus_lastsaved']) }}
