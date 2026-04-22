{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5AUDVALUES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AVA_LASTSAVED'], type='timestamp_ntz', alias='ava_lastsaved') }},
        {{ trx_json_extract('src', ['AVA_PRIMARYID'], type='varchar', alias='ava_primaryid') }},
        {{ trx_json_extract('src', ['AVA_SECONDARYID'], type='varchar', alias='ava_secondaryid') }},
        {{ trx_json_extract('src', ['AVA_FROM'], type='varchar', alias='ava_from') }},
        {{ trx_json_extract('src', ['AVA_TO'], type='varchar', alias='ava_to') }},
        {{ trx_json_extract('src', ['AVA_CHANGED'], type='timestamp_ntz', alias='ava_changed') }},
        {{ trx_json_extract('src', ['AVA_MODIFIEDBY'], type='varchar', alias='ava_modifiedby') }},
        {{ trx_json_extract('src', ['AVA_FUNCTION'], type='varchar', alias='ava_function') }},
        {{ trx_json_extract('src', ['AVA_UPDATED'], type='varchar', alias='ava_updated') }},
        {{ trx_json_extract('src', ['AVA_INSERTED'], type='varchar', alias='ava_inserted') }},
        {{ trx_json_extract('src', ['AVA_DELETED'], type='varchar', alias='ava_deleted') }},
        {{ trx_json_extract('src', ['AVA_UPDATECOUNT'], type='float', alias='ava_updatecount') }},
        {{ trx_json_extract('src', ['AVA_SCODE'], type='varchar', alias='ava_scode') }},
        {{ trx_json_extract('src', ['AVA_MOBILE'], type='varchar', alias='ava_mobile') }},
        {{ trx_json_extract('src', ['AVA_PRIMARYID2'], type='varchar', alias='ava_primaryid2') }},
        {{ trx_json_extract('src', ['AVA_ATTRIBUTE'], type='float', alias='ava_attribute') }},
        {{ trx_json_extract('src', ['AVA_TABLE'], type='varchar', alias='ava_table') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5audvalues') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ava_lastsaved']) }}
