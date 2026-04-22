{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5JOBPRRESPTIME',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['JBP_WOTYPE'], type='varchar', alias='jbp_wotype') }},
        {{ trx_json_extract('src', ['JBP_CONTRACTOR'], type='varchar', alias='jbp_contractor') }},
        {{ trx_json_extract('src', ['JBP_WORESPONSETIME'], type='float', alias='jbp_woresponsetime') }},
        {{ trx_json_extract('src', ['JBP_WOCOMPLETIONTIME'], type='float', alias='jbp_wocompletiontime') }},
        {{ trx_json_extract('src', ['JBP_WORESOLUTIONTIME'], type='float', alias='jbp_woresolutiontime') }},
        {{ trx_json_extract('src', ['JBP_BUSINESSDAYS'], type='varchar', alias='jbp_businessdays') }},
        {{ trx_json_extract('src', ['JBP_ORGANIZATION'], type='varchar', alias='jbp_organization') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['JBP_WOPRIORITY'], type='varchar', alias='jbp_wopriority') }},
        {{ trx_json_extract('src', ['JBP_WOPRIORITY_DESC'], type='varchar', alias='jbp_wopriority_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5jobprresptime') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['jbp_wopriority', 'jbp_wotype', 'jbp_contractor', 'jbp_organization']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
