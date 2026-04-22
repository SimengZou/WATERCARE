{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PPMPLAN',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PMP_LASTSAVED'], type='timestamp_ntz', alias='pmp_lastsaved') }},
        {{ trx_json_extract('src', ['PMP_DESC'], type='varchar', alias='pmp_desc') }},
        {{ trx_json_extract('src', ['PMP_ORG'], type='varchar', alias='pmp_org') }},
        {{ trx_json_extract('src', ['PMP_CLASS'], type='varchar', alias='pmp_class') }},
        {{ trx_json_extract('src', ['PMP_OBJECTCLASS'], type='varchar', alias='pmp_objectclass') }},
        {{ trx_json_extract('src', ['PMP_OBJECTCLASS_ORG'], type='varchar', alias='pmp_objectclass_org') }},
        {{ trx_json_extract('src', ['PMP_UPDATECOUNT'], type='float', alias='pmp_updatecount') }},
        {{ trx_json_extract('src', ['PMP_CODE'], type='varchar', alias='pmp_code') }},
        {{ trx_json_extract('src', ['PMP_CLASS_ORG'], type='varchar', alias='pmp_class_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5ppmplan') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pmp_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pmp_lastsaved']) }}
