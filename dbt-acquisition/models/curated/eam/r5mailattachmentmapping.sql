{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MAILATTACHMENTMAPPING',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MAM_LASTSAVED'], type='timestamp_ntz', alias='mam_lastsaved') }},
        {{ trx_json_extract('src', ['MAM_DOCPK'], type='varchar', alias='mam_docpk') }},
        {{ trx_json_extract('src', ['MAM_TABLE'], type='varchar', alias='mam_table') }},
        {{ trx_json_extract('src', ['MAM_RENTITY'], type='varchar', alias='mam_rentity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mailattachmentmapping') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mam_lastsaved']) }}
