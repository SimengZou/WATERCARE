{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PRODUCTVERSIONHISTORY',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PVH_LASTSAVED'], type='timestamp_ntz', alias='pvh_lastsaved') }},
        {{ trx_json_extract('src', ['PVH_CHANGED'], type='timestamp_ntz', alias='pvh_changed') }},
        {{ trx_json_extract('src', ['PVH_DESC'], type='varchar', alias='pvh_desc') }},
        {{ trx_json_extract('src', ['PVH_VERSION'], type='varchar', alias='pvh_version') }},
        {{ trx_json_extract('src', ['PVH_PATCH'], type='varchar', alias='pvh_patch') }},
        {{ trx_json_extract('src', ['PVH_UPDATECOUNT'], type='float', alias='pvh_updatecount') }},
        {{ trx_json_extract('src', ['PVH_BUILDDATE'], type='varchar', alias='pvh_builddate') }},
        {{ trx_json_extract('src', ['PVH_PRODUCTCODE'], type='varchar', alias='pvh_productcode') }},
        {{ trx_json_extract('src', ['PVH_BUILD'], type='varchar', alias='pvh_build') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5productversionhistory') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pvh_productcode', 'pvh_changed']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pvh_lastsaved']) }}
