{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5URLPARAMETERS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['URL_LASTSAVED'], type='timestamp_ntz', alias='url_lastsaved') }},
        {{ trx_json_extract('src', ['URL_TAB'], type='varchar', alias='url_tab') }},
        {{ trx_json_extract('src', ['URL_PARAMETERNAME'], type='varchar', alias='url_parametername') }},
        {{ trx_json_extract('src', ['URL_ALTERNATEPARAMETERNAME'], type='varchar', alias='url_alternateparametername') }},
        {{ trx_json_extract('src', ['URL_SYSTEM'], type='varchar', alias='url_system') }},
        {{ trx_json_extract('src', ['URL_ACTIVE'], type='varchar', alias='url_active') }},
        {{ trx_json_extract('src', ['URL_UPDATECOUNT'], type='float', alias='url_updatecount') }},
        {{ trx_json_extract('src', ['URL_USEFIELDVALUE'], type='varchar', alias='url_usefieldvalue') }},
        {{ trx_json_extract('src', ['URL_USERFUNCTION'], type='varchar', alias='url_userfunction') }},
        {{ trx_json_extract('src', ['URL_PARAMETERVALUE'], type='varchar', alias='url_parametervalue') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5urlparameters') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['url_userfunction', 'url_tab', 'url_parametername']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['url_lastsaved']) }}
