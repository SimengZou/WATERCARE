{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SCHEDGROUPS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SCG_LASTSAVED'], type='timestamp_ntz', alias='scg_lastsaved') }},
        {{ trx_json_extract('src', ['SCG_CLASS'], type='varchar', alias='scg_class') }},
        {{ trx_json_extract('src', ['SCG_ORG'], type='varchar', alias='scg_org') }},
        {{ trx_json_extract('src', ['SCG_CLASS_ORG'], type='varchar', alias='scg_class_org') }},
        {{ trx_json_extract('src', ['SCG_CODE'], type='varchar', alias='scg_code') }},
        {{ trx_json_extract('src', ['SCG_NOTUSED'], type='varchar', alias='scg_notused') }},
        {{ trx_json_extract('src', ['SCG_PROFILEPICTURE'], type='varchar', alias='scg_profilepicture') }},
        {{ trx_json_extract('src', ['SCG_DESC'], type='varchar', alias='scg_desc') }},
        {{ trx_json_extract('src', ['SCG_UPDATECOUNT'], type='float', alias='scg_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5schedgroups') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['scg_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['scg_lastsaved']) }}
