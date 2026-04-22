{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CLOSINGCODEHIERARCHY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CCH_LASTSAVED'], type='timestamp_ntz', alias='cch_lastsaved') }},
        {{ trx_json_extract('src', ['CCH_PARENTCLOSINGCODETYPE'], type='varchar', alias='cch_parentclosingcodetype') }},
        {{ trx_json_extract('src', ['CCH_CHILDCLOSINGCODE'], type='varchar', alias='cch_childclosingcode') }},
        {{ trx_json_extract('src', ['CCH_CHILDCLOSINGCODETYPE'], type='varchar', alias='cch_childclosingcodetype') }},
        {{ trx_json_extract('src', ['CCH_REPLDEPT'], type='varchar', alias='cch_repldept') }},
        {{ trx_json_extract('src', ['CCH_OBJECT_ORG'], type='varchar', alias='cch_object_org') }},
        {{ trx_json_extract('src', ['CCH_UPDATECOUNT'], type='float', alias='cch_updatecount') }},
        {{ trx_json_extract('src', ['CCH_PARENTCLOSINGCODE'], type='varchar', alias='cch_parentclosingcode') }},
        {{ trx_json_extract('src', ['CCH_OBJECT'], type='varchar', alias='cch_object') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5closingcodehierarchy') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cch_parentclosingcode', 'cch_parentclosingcodetype', 'cch_childclosingcode', 'cch_childclosingcodetype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cch_lastsaved']) }}
