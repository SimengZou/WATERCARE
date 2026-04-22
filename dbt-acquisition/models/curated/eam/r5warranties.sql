{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WARRANTIES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WAR_LASTSAVED'], type='timestamp_ntz', alias='war_lastsaved') }},
        {{ trx_json_extract('src', ['WAR_OBTYPE'], type='varchar', alias='war_obtype') }},
        {{ trx_json_extract('src', ['WAR_OBJECT'], type='varchar', alias='war_object') }},
        {{ trx_json_extract('src', ['WAR_OBJECT_ORG'], type='varchar', alias='war_object_org') }},
        {{ trx_json_extract('src', ['WAR_SUPPLIER'], type='varchar', alias='war_supplier') }},
        {{ trx_json_extract('src', ['WAR_SUPPLIER_ORG'], type='varchar', alias='war_supplier_org') }},
        {{ trx_json_extract('src', ['WAR_START'], type='timestamp_ntz', alias='war_start') }},
        {{ trx_json_extract('src', ['WAR_EXPIRATION'], type='timestamp_ntz', alias='war_expiration') }},
        {{ trx_json_extract('src', ['WAR_ID'], type='varchar', alias='war_id') }},
        {{ trx_json_extract('src', ['WAR_DESC'], type='varchar', alias='war_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5warranties') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['war_lastsaved']) }}
