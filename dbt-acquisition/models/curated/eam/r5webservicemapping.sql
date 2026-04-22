{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WEBSERVICEMAPPING',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WSG_LASTSAVED'], type='timestamp_ntz', alias='wsg_lastsaved') }},
        {{ trx_json_extract('src', ['WSG_TAB'], type='varchar', alias='wsg_tab') }},
        {{ trx_json_extract('src', ['WSG_WSNAME'], type='varchar', alias='wsg_wsname') }},
        {{ trx_json_extract('src', ['WSG_ACTION'], type='varchar', alias='wsg_action') }},
        {{ trx_json_extract('src', ['WSG_PACKAGENAME'], type='varchar', alias='wsg_packagename') }},
        {{ trx_json_extract('src', ['WSG_ORGXPATH'], type='varchar', alias='wsg_orgxpath') }},
        {{ trx_json_extract('src', ['WSG_UPDATECOUNT'], type='float', alias='wsg_updatecount') }},
        {{ trx_json_extract('src', ['WSG_FUNCTION'], type='varchar', alias='wsg_function') }},
        {{ trx_json_extract('src', ['WSG_INTERFACECODE'], type='varchar', alias='wsg_interfacecode') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5webservicemapping') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wsg_lastsaved']) }}
