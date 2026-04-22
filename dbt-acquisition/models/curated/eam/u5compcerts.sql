{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5COMPCERTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CMP_PK'], type='varchar', alias='cmp_pk') }},
        {{ trx_json_extract('src', ['CMP_TYPE'], type='varchar', alias='cmp_type') }},
        {{ trx_json_extract('src', ['CMP_ISSUER'], type='varchar', alias='cmp_issuer') }},
        {{ trx_json_extract('src', ['CMP_CERTIFIER'], type='varchar', alias='cmp_certifier') }},
        {{ trx_json_extract('src', ['CMP_CERTNUM'], type='varchar', alias='cmp_certnum') }},
        {{ trx_json_extract('src', ['CMP_ISSUEDATE'], type='timestamp_ntz', alias='cmp_issuedate') }},
        {{ trx_json_extract('src', ['CMP_EXPDATE'], type='timestamp_ntz', alias='cmp_expdate') }},
        {{ trx_json_extract('src', ['CMP_SCOPE'], type='varchar', alias='cmp_scope') }},
        {{ trx_json_extract('src', ['CMP_STATUS'], type='varchar', alias='cmp_status') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['CMP_OBJECT'], type='varchar', alias='cmp_object') }},
        {{ trx_json_extract('src', ['CMP_OBJECT_ORG'], type='varchar', alias='cmp_object_org') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5compcerts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cmp_object', 'cmp_object_org', 'cmp_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
