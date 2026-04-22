{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTIONPULSE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ALI_LASTSAVED'], type='timestamp_ntz', alias='ali_lastsaved') }},
        {{ trx_json_extract('src', ['ALI_DELAY'], type='float', alias='ali_delay') }},
        {{ trx_json_extract('src', ['ALI_DELAYUOM'], type='varchar', alias='ali_delayuom') }},
        {{ trx_json_extract('src', ['ALI_DESCRIPTIONFIELDID'], type='float', alias='ali_descriptionfieldid') }},
        {{ trx_json_extract('src', ['ALI_UPDATECOUNT'], type='float', alias='ali_updatecount') }},
        {{ trx_json_extract('src', ['ALI_ALERT'], type='varchar', alias='ali_alert') }},
        {{ trx_json_extract('src', ['ALI_RECIPIENTFIELDID'], type='float', alias='ali_recipientfieldid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alertionpulse') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ali_alert']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ali_lastsaved']) }}
