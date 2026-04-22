{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CURRENCIES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CUR_LASTSAVED'], type='timestamp_ntz', alias='cur_lastsaved') }},
        {{ trx_json_extract('src', ['CUR_DESC'], type='varchar', alias='cur_desc') }},
        {{ trx_json_extract('src', ['CUR_CLASS'], type='varchar', alias='cur_class') }},
        {{ trx_json_extract('src', ['CUR_SOURCESYSTEM'], type='varchar', alias='cur_sourcesystem') }},
        {{ trx_json_extract('src', ['CUR_SOURCECODE'], type='varchar', alias='cur_sourcecode') }},
        {{ trx_json_extract('src', ['CUR_INTERFACE'], type='timestamp_ntz', alias='cur_interface') }},
        {{ trx_json_extract('src', ['CUR_DUAL'], type='float', alias='cur_dual') }},
        {{ trx_json_extract('src', ['CUR_CLASS_ORG'], type='varchar', alias='cur_class_org') }},
        {{ trx_json_extract('src', ['CUR_UPDATECOUNT'], type='float', alias='cur_updatecount') }},
        {{ trx_json_extract('src', ['CUR_CREATED'], type='timestamp_ntz', alias='cur_created') }},
        {{ trx_json_extract('src', ['CUR_UPDATED'], type='timestamp_ntz', alias='cur_updated') }},
        {{ trx_json_extract('src', ['CUR_CODE'], type='varchar', alias='cur_code') }},
        {{ trx_json_extract('src', ['CUR_NOTUSED'], type='varchar', alias='cur_notused') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5currencies') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cur_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cur_lastsaved']) }}
