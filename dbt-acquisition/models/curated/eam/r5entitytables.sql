{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ENTITYTABLES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ETT_LASTSAVED'], type='timestamp_ntz', alias='ett_lastsaved') }},
        {{ trx_json_extract('src', ['ETT_MOS'], type='varchar', alias='ett_mos') }},
        {{ trx_json_extract('src', ['ETT_UPDATED'], type='timestamp_ntz', alias='ett_updated') }},
        {{ trx_json_extract('src', ['ETT_RENTITY'], type='varchar', alias='ett_rentity') }},
        {{ trx_json_extract('src', ['ETT_UPDATECOUNT'], type='float', alias='ett_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5entitytables') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ett_rentity']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ett_lastsaved']) }}
