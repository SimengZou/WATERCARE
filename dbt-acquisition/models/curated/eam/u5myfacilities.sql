{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5MYFACILITIES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['MFA_FACCODE'], type='varchar', alias='mfa_faccode') }},
        {{ trx_json_extract('src', ['MFA_FACDESC'], type='varchar', alias='mfa_facdesc') }},
        {{ trx_json_extract('src', ['MFA_MRC'], type='varchar', alias='mfa_mrc') }},
        {{ trx_json_extract('src', ['MFA_MRCDESC'], type='varchar', alias='mfa_mrcdesc') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['MFA_USER'], type='varchar', alias='mfa_user') }},
        {{ trx_json_extract('src', ['MFA_FACORG'], type='varchar', alias='mfa_facorg') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5myfacilities') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mfa_user', 'mfa_faccode']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
