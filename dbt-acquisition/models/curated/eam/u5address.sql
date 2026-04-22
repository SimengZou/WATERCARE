{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5ADDRESS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['ADR_ORG'], type='varchar', alias='adr_org') }},
        {{ trx_json_extract('src', ['ADR_HOUSENO'], type='varchar', alias='adr_houseno') }},
        {{ trx_json_extract('src', ['ADR_STREET'], type='varchar', alias='adr_street') }},
        {{ trx_json_extract('src', ['ADR_SUBURB'], type='varchar', alias='adr_suburb') }},
        {{ trx_json_extract('src', ['ADR_STATE'], type='varchar', alias='adr_state') }},
        {{ trx_json_extract('src', ['ADR_POSTALCODE'], type='varchar', alias='adr_postalcode') }},
        {{ trx_json_extract('src', ['ADR_STREETTYPE'], type='varchar', alias='adr_streettype') }},
        {{ trx_json_extract('src', ['ADR_SUBDIVISION'], type='varchar', alias='adr_subdivision') }},
        {{ trx_json_extract('src', ['ADR_SERVICEAREA'], type='varchar', alias='adr_servicearea') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['ADR_KEY'], type='varchar', alias='adr_key') }},
        {{ trx_json_extract('src', ['ADR_FLAT'], type='varchar', alias='adr_flat') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5address') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['adr_key']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
