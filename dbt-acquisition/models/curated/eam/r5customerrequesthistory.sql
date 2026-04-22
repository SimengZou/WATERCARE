{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CUSTOMERREQUESTHISTORY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CRH_LASTSAVED'], type='timestamp_ntz', alias='crh_lastsaved') }},
        {{ trx_json_extract('src', ['CRH_CALLCENTERCODE'], type='varchar', alias='crh_callcentercode') }},
        {{ trx_json_extract('src', ['CRH_CALLCENTER_ORG'], type='varchar', alias='crh_callcenter_org') }},
        {{ trx_json_extract('src', ['CRH_REQDATE'], type='timestamp_ntz', alias='crh_reqdate') }},
        {{ trx_json_extract('src', ['CRH_EVENTTYPE'], type='varchar', alias='crh_eventtype') }},
        {{ trx_json_extract('src', ['CRH_USERCODE'], type='varchar', alias='crh_usercode') }},
        {{ trx_json_extract('src', ['CRH_OLDVALUE'], type='varchar', alias='crh_oldvalue') }},
        {{ trx_json_extract('src', ['CRH_NEWVALUE'], type='varchar', alias='crh_newvalue') }},
        {{ trx_json_extract('src', ['CRH_UPDATECOUNT'], type='float', alias='crh_updatecount') }},
        {{ trx_json_extract('src', ['CRH_COMMENT'], type='varchar', alias='crh_comment') }},
        {{ trx_json_extract('src', ['CRH_EVENT'], type='varchar', alias='crh_event') }},
        {{ trx_json_extract('src', ['CRH_RENTITY'], type='varchar', alias='crh_rentity') }},
        {{ trx_json_extract('src', ['CRH_PK'], type='varchar', alias='crh_pk') }},
        {{ trx_json_extract('src', ['CRH_FIELD'], type='varchar', alias='crh_field') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5customerrequesthistory') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['crh_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['crh_lastsaved']) }}
