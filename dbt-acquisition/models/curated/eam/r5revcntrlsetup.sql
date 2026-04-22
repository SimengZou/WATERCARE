{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REVCNTRLSETUP',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RCS_LASTSAVED'], type='timestamp_ntz', alias='rcs_lastsaved') }},
        {{ trx_json_extract('src', ['RCS_ELEMENTID'], type='varchar', alias='rcs_elementid') }},
        {{ trx_json_extract('src', ['RCS_PROTECTED'], type='varchar', alias='rcs_protected') }},
        {{ trx_json_extract('src', ['RCS_UPDATECOUNT'], type='float', alias='rcs_updatecount') }},
        {{ trx_json_extract('src', ['RCS_PAGENAME'], type='varchar', alias='rcs_pagename') }},
        {{ trx_json_extract('src', ['RCS_XPATH'], type='varchar', alias='rcs_xpath') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5revcntrlsetup') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rcs_pagename', 'rcs_elementid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rcs_lastsaved']) }}
