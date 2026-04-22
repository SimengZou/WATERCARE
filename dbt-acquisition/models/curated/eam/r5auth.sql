{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5AUTH',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AUT_LASTSAVED'], type='timestamp_ntz', alias='aut_lastsaved') }},
        {{ trx_json_extract('src', ['AUT_USER'], type='varchar', alias='aut_user') }},
        {{ trx_json_extract('src', ['AUT_ENTITY'], type='varchar', alias='aut_entity') }},
        {{ trx_json_extract('src', ['AUT_RENTITY'], type='varchar', alias='aut_rentity') }},
        {{ trx_json_extract('src', ['AUT_STATUS'], type='varchar', alias='aut_status') }},
        {{ trx_json_extract('src', ['AUT_TYPE'], type='varchar', alias='aut_type') }},
        {{ trx_json_extract('src', ['AUT_UPDATECOUNT'], type='float', alias='aut_updatecount') }},
        {{ trx_json_extract('src', ['AUT_CREATED'], type='timestamp_ntz', alias='aut_created') }},
        {{ trx_json_extract('src', ['AUT_UPDATED'], type='timestamp_ntz', alias='aut_updated') }},
        {{ trx_json_extract('src', ['AUT_GROUP'], type='varchar', alias='aut_group') }},
        {{ trx_json_extract('src', ['AUT_STATNEW'], type='varchar', alias='aut_statnew') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5auth') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['aut_group', 'aut_user', 'aut_entity', 'aut_status', 'aut_statnew']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['aut_lastsaved']) }}
