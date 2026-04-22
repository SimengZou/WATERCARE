{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5CLAIMSETUP',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['CLS_COSTITEM'], type='varchar', alias='cls_costitem') }},
        {{ trx_json_extract('src', ['CLS_CONTRACT'], type='varchar', alias='cls_contract') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['CLS_PROJACTIVITY'], type='varchar', alias='cls_projactivity') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CLS_STAGINGCOSTCENTER'], type='varchar', alias='cls_stagingcostcenter') }},
        {{ trx_json_extract('src', ['CLS_SUPPLIER'], type='varchar', alias='cls_supplier') }},
        {{ trx_json_extract('src', ['CLS_PROJECT'], type='varchar', alias='cls_project') }},
        {{ trx_json_extract('src', ['CLS_CODE'], type='varchar', alias='cls_code') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5claimsetup') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cls_code', 'cls_costitem', 'cls_contract']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
