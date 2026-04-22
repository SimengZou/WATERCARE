{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CUSTOMTEXTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CTT_LASTSAVED'], type='timestamp_ntz', alias='ctt_lastsaved') }},
        {{ trx_json_extract('src', ['CTT_LANG'], type='varchar', alias='ctt_lang') }},
        {{ trx_json_extract('src', ['CTT_LENGTH'], type='float', alias='ctt_length') }},
        {{ trx_json_extract('src', ['CTT_ORIGTEXT'], type='varchar', alias='ctt_origtext') }},
        {{ trx_json_extract('src', ['CTT_DATE'], type='timestamp_ntz', alias='ctt_date') }},
        {{ trx_json_extract('src', ['CTT_UPDATECOUNT'], type='float', alias='ctt_updatecount') }},
        {{ trx_json_extract('src', ['CTT_POOL'], type='varchar', alias='ctt_pool') }},
        {{ trx_json_extract('src', ['CTT_TEXT'], type='varchar', alias='ctt_text') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5customtexts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ctt_pool', 'ctt_lang', 'ctt_length']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ctt_lastsaved']) }}
