{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSPNOTALLOWED',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WPA_LASTSAVED'], type='timestamp_ntz', alias='wpa_lastsaved') }},
        {{ trx_json_extract('src', ['WPA_TAB'], type='varchar', alias='wpa_tab') }},
        {{ trx_json_extract('src', ['WPA_ALLTABS'], type='varchar', alias='wpa_alltabs') }},
        {{ trx_json_extract('src', ['WPA_FUNCTION'], type='varchar', alias='wpa_function') }},
        {{ trx_json_extract('src', ['WPA_ACTION'], type='varchar', alias='wpa_action') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wspnotallowed') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wpa_function', 'wpa_tab', 'wpa_action', 'wpa_alltabs']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wpa_lastsaved']) }}
