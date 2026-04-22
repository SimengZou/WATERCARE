{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERLINEARPREFERENCES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ULP_LASTSAVED'], type='timestamp_ntz', alias='ulp_lastsaved') }},
        {{ trx_json_extract('src', ['ULP_LOPOINTSOFINT'], type='varchar', alias='ulp_lopointsofint') }},
        {{ trx_json_extract('src', ['ULP_LORELATEDEQ'], type='varchar', alias='ulp_lorelatedeq') }},
        {{ trx_json_extract('src', ['ULP_LORELATEDPART'], type='varchar', alias='ulp_lorelatedpart') }},
        {{ trx_json_extract('src', ['ULP_LODEFAULTCOLOR'], type='varchar', alias='ulp_lodefaultcolor') }},
        {{ trx_json_extract('src', ['ULP_LODEFAULTFILTER'], type='varchar', alias='ulp_lodefaultfilter') }},
        {{ trx_json_extract('src', ['ULP_LOINCLUDERELATED'], type='varchar', alias='ulp_loincluderelated') }},
        {{ trx_json_extract('src', ['ULP_CREATED'], type='timestamp_ntz', alias='ulp_created') }},
        {{ trx_json_extract('src', ['ULP_CREATEDBY'], type='varchar', alias='ulp_createdby') }},
        {{ trx_json_extract('src', ['ULP_UPDATED'], type='timestamp_ntz', alias='ulp_updated') }},
        {{ trx_json_extract('src', ['ULP_UPDATEDBY'], type='varchar', alias='ulp_updatedby') }},
        {{ trx_json_extract('src', ['ULP_UPDATECOUNT'], type='float', alias='ulp_updatecount') }},
        {{ trx_json_extract('src', ['ULP_LOROUTEANDSEGMENT'], type='varchar', alias='ulp_lorouteandsegment') }},
        {{ trx_json_extract('src', ['ULP_CODE'], type='varchar', alias='ulp_code') }},
        {{ trx_json_extract('src', ['ULP_DESC'], type='varchar', alias='ulp_desc') }},
        {{ trx_json_extract('src', ['ULP_DEFAULT'], type='varchar', alias='ulp_default') }},
        {{ trx_json_extract('src', ['ULP_HIDESEGMENTS'], type='varchar', alias='ulp_hidesegments') }},
        {{ trx_json_extract('src', ['ULP_HIDEROUES'], type='varchar', alias='ulp_hideroues') }},
        {{ trx_json_extract('src', ['ULP_GROUPSEGMENTS'], type='varchar', alias='ulp_groupsegments') }},
        {{ trx_json_extract('src', ['ULP_USERCODE'], type='varchar', alias='ulp_usercode') }},
        {{ trx_json_extract('src', ['ULP_LOLINEARREF'], type='varchar', alias='ulp_lolinearref') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userlinearpreferences') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ulp_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ulp_lastsaved']) }}
