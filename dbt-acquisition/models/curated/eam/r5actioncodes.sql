{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ACTIONCODES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ACC_LASTSAVED'], type='timestamp_ntz', alias='acc_lastsaved') }},
        {{ trx_json_extract('src', ['ACC_DESC'], type='varchar', alias='acc_desc') }},
        {{ trx_json_extract('src', ['ACC_CLASS'], type='varchar', alias='acc_class') }},
        {{ trx_json_extract('src', ['ACC_GEN'], type='varchar', alias='acc_gen') }},
        {{ trx_json_extract('src', ['ACC_CLASS_ORG'], type='varchar', alias='acc_class_org') }},
        {{ trx_json_extract('src', ['ACC_UPDATECOUNT'], type='float', alias='acc_updatecount') }},
        {{ trx_json_extract('src', ['ACC_UPDATED'], type='timestamp_ntz', alias='acc_updated') }},
        {{ trx_json_extract('src', ['ACC_PARTFAILURE'], type='varchar', alias='acc_partfailure') }},
        {{ trx_json_extract('src', ['ACC_NOTUSED'], type='varchar', alias='acc_notused') }},
        {{ trx_json_extract('src', ['ACC_ENABLEWORKORDERS'], type='varchar', alias='acc_enableworkorders') }},
        {{ trx_json_extract('src', ['ACC_GROUP'], type='varchar', alias='acc_group') }},
        {{ trx_json_extract('src', ['ACC_CODE'], type='varchar', alias='acc_code') }},
        {{ trx_json_extract('src', ['ACC_CREATED'], type='timestamp_ntz', alias='acc_created') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5actioncodes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['acc_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['acc_lastsaved']) }}
