{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5NONCONFORMITYSETUP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['NCP_LASTSAVED'], type='timestamp_ntz', alias='ncp_lastsaved') }},
        {{ trx_json_extract('src', ['NCP_COPYFROM'], type='varchar', alias='ncp_copyfrom') }},
        {{ trx_json_extract('src', ['NCP_PROTECTOBSDATAONHDR'], type='varchar', alias='ncp_protectobsdataonhdr') }},
        {{ trx_json_extract('src', ['NCP_SYNCHRONIZE'], type='varchar', alias='ncp_synchronize') }},
        {{ trx_json_extract('src', ['NCP_MERGERESTRICTIONS'], type='varchar', alias='ncp_mergerestrictions') }},
        {{ trx_json_extract('src', ['NCP_UPDATEDBY'], type='varchar', alias='ncp_updatedby') }},
        {{ trx_json_extract('src', ['NCP_UPDATED'], type='timestamp_ntz', alias='ncp_updated') }},
        {{ trx_json_extract('src', ['NCP_USEOBSSTATUS'], type='varchar', alias='ncp_useobsstatus') }},
        {{ trx_json_extract('src', ['NCP_SUPERSEDEDSTATUS'], type='varchar', alias='ncp_supersededstatus') }},
        {{ trx_json_extract('src', ['NCP_REINSPECTSTATUS'], type='varchar', alias='ncp_reinspectstatus') }},
        {{ trx_json_extract('src', ['NCP_AUTOAPPROVESTATUS'], type='varchar', alias='ncp_autoapprovestatus') }},
        {{ trx_json_extract('src', ['NCP_AUTOSKIPSTATUS'], type='varchar', alias='ncp_autoskipstatus') }},
        {{ trx_json_extract('src', ['NCP_INCLUDECONDITION'], type='varchar', alias='ncp_includecondition') }},
        {{ trx_json_extract('src', ['NCP_PREVOBSCOUNT'], type='float', alias='ncp_prevobscount') }},
        {{ trx_json_extract('src', ['NCP_UPDATECOUNT'], type='float', alias='ncp_updatecount') }},
        {{ trx_json_extract('src', ['NCP_CREATEFROMCHECKLIST'], type='varchar', alias='ncp_createfromchecklist') }},
        {{ trx_json_extract('src', ['NCP_MASSACKNOWLEDGEALLOWED'], type='varchar', alias='ncp_massacknowledgeallowed') }},
        {{ trx_json_extract('src', ['NCP_COPYSEVERITY'], type='varchar', alias='ncp_copyseverity') }},
        {{ trx_json_extract('src', ['NCP_COPYINTENSITY'], type='varchar', alias='ncp_copyintensity') }},
        {{ trx_json_extract('src', ['NCP_COPYSIZE'], type='varchar', alias='ncp_copysize') }},
        {{ trx_json_extract('src', ['NCP_COPYIMPORTANCE'], type='varchar', alias='ncp_copyimportance') }},
        {{ trx_json_extract('src', ['NCP_COPYNEXTINSPDATEOVERRIDE'], type='varchar', alias='ncp_copynextinspdateoverride') }},
        {{ trx_json_extract('src', ['NCP_COPYOBSERVATIONUDFS'], type='varchar', alias='ncp_copyobservationudfs') }},
        {{ trx_json_extract('src', ['NCP_ORG'], type='varchar', alias='ncp_org') }},
        {{ trx_json_extract('src', ['NCP_PROTECTHEADER'], type='varchar', alias='ncp_protectheader') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5nonconformitysetup') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ncp_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ncp_lastsaved']) }}
