{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IMPORTHEADERDATA',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IMH_LASTSAVED'], type='timestamp_ntz', alias='imh_lastsaved') }},
        {{ trx_json_extract('src', ['IMH_SESSIONID'], type='float', alias='imh_sessionid') }},
        {{ trx_json_extract('src', ['IMH_HEADERDATA'], type='varchar', alias='imh_headerdata') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5importheaderdata') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['imh_lastsaved']) }}
