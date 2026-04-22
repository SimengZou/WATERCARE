{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5JSPFIELDS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['JFD_LASTSAVED'], type='timestamp_ntz', alias='jfd_lastsaved') }},
        {{ trx_json_extract('src', ['JFD_FIELDS'], type='varchar', alias='jfd_fields') }},
        {{ trx_json_extract('src', ['JFD_JSINCLUDES'], type='varchar', alias='jfd_jsincludes') }},
        {{ trx_json_extract('src', ['JFD_UPDATECOUNT'], type='float', alias='jfd_updatecount') }},
        {{ trx_json_extract('src', ['JFD_JSPFILE'], type='varchar', alias='jfd_jspfile') }},
        {{ trx_json_extract('src', ['JFD_OTHERFIELDS'], type='varchar', alias='jfd_otherfields') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5jspfields') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['jfd_jspfile']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['jfd_lastsaved']) }}
