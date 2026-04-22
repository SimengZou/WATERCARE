{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTMAIL',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ALM_LASTSAVED'], type='timestamp_ntz', alias='alm_lastsaved') }},
        {{ trx_json_extract('src', ['ALM_TEMPLATE'], type='varchar', alias='alm_template') }},
        {{ trx_json_extract('src', ['ALM_DELAYUOM'], type='varchar', alias='alm_delayuom') }},
        {{ trx_json_extract('src', ['ALM_UPDATECOUNT'], type='float', alias='alm_updatecount') }},
        {{ trx_json_extract('src', ['ALM_ALERT'], type='varchar', alias='alm_alert') }},
        {{ trx_json_extract('src', ['ALM_DELAY'], type='float', alias='alm_delay') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alertmail') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['alm_alert', 'alm_template']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['alm_lastsaved']) }}
