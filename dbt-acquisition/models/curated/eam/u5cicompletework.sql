{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5CICOMPLETEWORK',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['CIC_RESULTWONUM'], type='varchar', alias='cic_resultwonum') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CIC_TYPE'], type='varchar', alias='cic_type') }},
        {{ trx_json_extract('src', ['CIC_TRANSID'], type='varchar', alias='cic_transid') }},
        {{ trx_json_extract('src', ['CIC_REFERENCE'], type='varchar', alias='cic_reference') }},
        {{ trx_json_extract('src', ['CIC_CONTRACTORCODE'], type='varchar', alias='cic_contractorcode') }},
        {{ trx_json_extract('src', ['CIC_OBJECT'], type='varchar', alias='cic_object') }},
        {{ trx_json_extract('src', ['CIC_COMMENT'], type='varchar', alias='cic_comment') }},
        {{ trx_json_extract('src', ['CIC_ACTIVITYCODE'], type='varchar', alias='cic_activitycode') }},
        {{ trx_json_extract('src', ['CIC_ASSETCONDITION'], type='varchar', alias='cic_assetcondition') }},
        {{ trx_json_extract('src', ['CIC_CREW'], type='varchar', alias='cic_crew') }},
        {{ trx_json_extract('src', ['CIC_INITIATEDDATE'], type='timestamp_ntz', alias='cic_initiateddate') }},
        {{ trx_json_extract('src', ['CIC_STARTDATE'], type='timestamp_ntz', alias='cic_startdate') }},
        {{ trx_json_extract('src', ['CIC_COMPLETEDDATE'], type='timestamp_ntz', alias='cic_completeddate') }},
        {{ trx_json_extract('src', ['CIC_RESULTCODE'], type='varchar', alias='cic_resultcode') }},
        {{ trx_json_extract('src', ['CIC_VARIATIONNO'], type='varchar', alias='cic_variationno') }},
        {{ trx_json_extract('src', ['CIC_EVENT'], type='varchar', alias='cic_event') }},
        {{ trx_json_extract('src', ['CIC_COMPLETEBY'], type='varchar', alias='cic_completeby') }},
        {{ trx_json_extract('src', ['CIC_RESULTS'], type='varchar', alias='cic_results') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5cicompletework') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cic_transid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
