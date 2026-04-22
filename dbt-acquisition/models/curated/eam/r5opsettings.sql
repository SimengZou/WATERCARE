{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5OPSETTINGS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OST_LASTSAVED'], type='timestamp_ntz', alias='ost_lastsaved') }},
        {{ trx_json_extract('src', ['OST_SETTING'], type='varchar', alias='ost_setting') }},
        {{ trx_json_extract('src', ['OST_AUDITUSER'], type='varchar', alias='ost_audituser') }},
        {{ trx_json_extract('src', ['OST_AUDITTIMESTAMP'], type='timestamp_ntz', alias='ost_audittimestamp') }},
        {{ trx_json_extract('src', ['OST_CURVALUE'], type='varchar', alias='ost_curvalue') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5opsettings') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ost_lastsaved']) }}
