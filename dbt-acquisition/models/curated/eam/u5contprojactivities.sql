{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5CONTPROJACTIVITIES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['CSA_ORG'], type='varchar', alias='csa_org') }},
        {{ trx_json_extract('src', ['CSA_SCHEDULEITEM'], type='varchar', alias='csa_scheduleitem') }},
        {{ trx_json_extract('src', ['CSA_PROJECT'], type='varchar', alias='csa_project') }},
        {{ trx_json_extract('src', ['CSA_ACTIVITY'], type='varchar', alias='csa_activity') }},
        {{ trx_json_extract('src', ['CSA_CONTRACTCODE'], type='varchar', alias='csa_contractcode') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CSA_SUPPLIER'], type='varchar', alias='csa_supplier') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['CSA_CONTRACT'], type='varchar', alias='csa_contract') }},
        {{ trx_json_extract('src', ['CSA_CONTRACTOR'], type='varchar', alias='csa_contractor') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5contprojactivities') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['csa_contract', 'csa_org', 'csa_scheduleitem']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
