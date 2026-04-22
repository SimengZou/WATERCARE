{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5GUCICOMPLETEWORK',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CIC_ACTIVITYCODE'], type='varchar', alias='cic_activitycode') }},
        {{ trx_json_extract('src', ['LASTSAVED'], type='date', alias='lastsaved') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5gucicompletework') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_load_datetime']) }}
