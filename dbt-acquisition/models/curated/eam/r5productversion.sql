{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PRODUCTVERSION',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PVS_LASTSAVED'], type='timestamp_ntz', alias='pvs_lastsaved') }},
        {{ trx_json_extract('src', ['PVS_PRODUCTDESC'], type='varchar', alias='pvs_productdesc') }},
        {{ trx_json_extract('src', ['PVS_VERSION'], type='varchar', alias='pvs_version') }},
        {{ trx_json_extract('src', ['PVS_PATCH'], type='varchar', alias='pvs_patch') }},
        {{ trx_json_extract('src', ['PVS_UPDATECOUNT'], type='float', alias='pvs_updatecount') }},
        {{ trx_json_extract('src', ['PVS_PRODUCTCODE'], type='varchar', alias='pvs_productcode') }},
        {{ trx_json_extract('src', ['PVS_BUILD'], type='varchar', alias='pvs_build') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5productversion') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pvs_productcode']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pvs_lastsaved']) }}
