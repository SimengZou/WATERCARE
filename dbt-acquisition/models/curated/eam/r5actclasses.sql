{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ACTCLASSES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ACL_LASTSAVED'], type='timestamp_ntz', alias='acl_lastsaved') }},
        {{ trx_json_extract('src', ['ACL_UPDATECOUNT'], type='float', alias='acl_updatecount') }},
        {{ trx_json_extract('src', ['ACL_ACTION'], type='varchar', alias='acl_action') }},
        {{ trx_json_extract('src', ['ACL_UPDATED'], type='timestamp_ntz', alias='acl_updated') }},
        {{ trx_json_extract('src', ['ACL_CLASS'], type='varchar', alias='acl_class') }},
        {{ trx_json_extract('src', ['ACL_CREATED'], type='timestamp_ntz', alias='acl_created') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5actclasses') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['acl_class', 'acl_action']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['acl_lastsaved']) }}
