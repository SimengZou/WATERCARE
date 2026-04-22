{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ORGANIZATIONOPTIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OPA_LASTSAVED'], type='timestamp_ntz', alias='opa_lastsaved') }},
        {{ trx_json_extract('src', ['OPA_ORG'], type='varchar', alias='opa_org') }},
        {{ trx_json_extract('src', ['OPA_SYSTEM'], type='varchar', alias='opa_system') }},
        {{ trx_json_extract('src', ['OPA_DESC'], type='varchar', alias='opa_desc') }},
        {{ trx_json_extract('src', ['OPA_COMMENT'], type='varchar', alias='opa_comment') }},
        {{ trx_json_extract('src', ['OPA_MODULE'], type='varchar', alias='opa_module') }},
        {{ trx_json_extract('src', ['OPA_TYPE'], type='varchar', alias='opa_type') }},
        {{ trx_json_extract('src', ['OPA_VALIDVALUES'], type='varchar', alias='opa_validvalues') }},
        {{ trx_json_extract('src', ['OPA_UPDATECOUNT'], type='float', alias='opa_updatecount') }},
        {{ trx_json_extract('src', ['OPA_CODE'], type='varchar', alias='opa_code') }},
        {{ trx_json_extract('src', ['OPA_FIXED'], type='varchar', alias='opa_fixed') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5organizationoptions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['opa_code', 'opa_org', 'opa_system']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['opa_lastsaved']) }}
