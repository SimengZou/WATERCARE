{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DLVCONCLAUSES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CCL_LASTSAVED'], type='timestamp_ntz', alias='ccl_lastsaved') }},
        {{ trx_json_extract('src', ['CCL_DESC'], type='varchar', alias='ccl_desc') }},
        {{ trx_json_extract('src', ['CCL_CLASS'], type='varchar', alias='ccl_class') }},
        {{ trx_json_extract('src', ['CCL_PARENT'], type='varchar', alias='ccl_parent') }},
        {{ trx_json_extract('src', ['CCL_LINE'], type='float', alias='ccl_line') }},
        {{ trx_json_extract('src', ['CCL_ORG'], type='varchar', alias='ccl_org') }},
        {{ trx_json_extract('src', ['CCL_CLASS_ORG'], type='varchar', alias='ccl_class_org') }},
        {{ trx_json_extract('src', ['CCL_UPDATECOUNT'], type='float', alias='ccl_updatecount') }},
        {{ trx_json_extract('src', ['CCL_NOTUSED'], type='varchar', alias='ccl_notused') }},
        {{ trx_json_extract('src', ['CCL_CODE'], type='varchar', alias='ccl_code') }},
        {{ trx_json_extract('src', ['CCL_SYSTEM'], type='varchar', alias='ccl_system') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dlvconclauses') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ccl_lastsaved']) }}
