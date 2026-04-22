{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5CLAIMCONFIG',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['CLC_CHILDWORATEAPP'], type='varchar', alias='clc_childworateapp') }},
        {{ trx_json_extract('src', ['CLC_COSTITEM'], type='varchar', alias='clc_costitem') }},
        {{ trx_json_extract('src', ['CLC_LNCOMPANY'], type='varchar', alias='clc_lncompany') }},
        {{ trx_json_extract('src', ['CLC_STGCOSTCODE'], type='varchar', alias='clc_stgcostcode') }},
        {{ trx_json_extract('src', ['CLC_PROJECTNUMBER'], type='varchar', alias='clc_projectnumber') }},
        {{ trx_json_extract('src', ['CLC_PROJECTACTIVITY'], type='varchar', alias='clc_projectactivity') }},
        {{ trx_json_extract('src', ['CLC_SUPPLIER'], type='varchar', alias='clc_supplier') }},
        {{ trx_json_extract('src', ['CLC_STORE'], type='varchar', alias='clc_store') }},
        {{ trx_json_extract('src', ['CLC_CREATOR'], type='varchar', alias='clc_creator') }},
        {{ trx_json_extract('src', ['CLC_REQUESTOR'], type='varchar', alias='clc_requestor') }},
        {{ trx_json_extract('src', ['CLC_REQUESTOR2'], type='varchar', alias='clc_requestor2') }},
        {{ trx_json_extract('src', ['CLC_PURCHASEOFFICE'], type='varchar', alias='clc_purchaseoffice') }},
        {{ trx_json_extract('src', ['CLC_CONTORDERTYPE'], type='varchar', alias='clc_contordertype') }},
        {{ trx_json_extract('src', ['CLC_NONCONTORDERTYPE'], type='varchar', alias='clc_noncontordertype') }},
        {{ trx_json_extract('src', ['CLC_ORDERSERIES'], type='varchar', alias='clc_orderseries') }},
        {{ trx_json_extract('src', ['CLC_EXCLUDEWOFROMLN'], type='varchar', alias='clc_excludewofromln') }},
        {{ trx_json_extract('src', ['CLC_EXCLUDEPOFROMLN'], type='varchar', alias='clc_excludepofromln') }},
        {{ trx_json_extract('src', ['CLC_SPLITINTERCOMPANYCOSTS'], type='varchar', alias='clc_splitintercompanycosts') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CLC_PMWORATEAPP'], type='varchar', alias='clc_pmworateapp') }},
        {{ trx_json_extract('src', ['CLC_ORG'], type='varchar', alias='clc_org') }},
        {{ trx_json_extract('src', ['CLC_CONTRACTOR'], type='varchar', alias='clc_contractor') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5claimconfig') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['clc_org', 'clc_contractor']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
