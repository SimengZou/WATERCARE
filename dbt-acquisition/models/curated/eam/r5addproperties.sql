{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ADDPROPERTIES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['APR_LASTSAVED'], type='timestamp_ntz', alias='apr_lastsaved') }},
        {{ trx_json_extract('src', ['APR_NONUPDCAT'], type='varchar', alias='apr_nonupdcat') }},
        {{ trx_json_extract('src', ['APR_CREATED'], type='timestamp_ntz', alias='apr_created') }},
        {{ trx_json_extract('src', ['APR_UPDATED'], type='timestamp_ntz', alias='apr_updated') }},
        {{ trx_json_extract('src', ['APR_GROUPLABEL'], type='varchar', alias='apr_grouplabel') }},
        {{ trx_json_extract('src', ['APR_STATUS'], type='varchar', alias='apr_status') }},
        {{ trx_json_extract('src', ['APR_CLASS_ORG'], type='varchar', alias='apr_class_org') }},
        {{ trx_json_extract('src', ['APR_PROPERTY'], type='varchar', alias='apr_property') }},
        {{ trx_json_extract('src', ['APR_RENTITY'], type='varchar', alias='apr_rentity') }},
        {{ trx_json_extract('src', ['APR_CLASS'], type='varchar', alias='apr_class') }},
        {{ trx_json_extract('src', ['APR_LINE'], type='float', alias='apr_line') }},
        {{ trx_json_extract('src', ['APR_UOM'], type='varchar', alias='apr_uom') }},
        {{ trx_json_extract('src', ['APR_LIST'], type='varchar', alias='apr_list') }},
        {{ trx_json_extract('src', ['APR_LISTVALID'], type='varchar', alias='apr_listvalid') }},
        {{ trx_json_extract('src', ['APR_REQUIRED'], type='varchar', alias='apr_required') }},
        {{ trx_json_extract('src', ['APR_WODISP'], type='varchar', alias='apr_wodisp') }},
        {{ trx_json_extract('src', ['APR_UPDATECOUNT'], type='float', alias='apr_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5addproperties') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['apr_property', 'apr_rentity', 'apr_class', 'apr_class_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['apr_lastsaved']) }}
