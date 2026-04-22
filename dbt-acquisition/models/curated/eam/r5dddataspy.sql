{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DDDATASPY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DDS_LASTSAVED'], type='timestamp_ntz', alias='dds_lastsaved') }},
        {{ trx_json_extract('src', ['DDS_AUTORUN'], type='varchar', alias='dds_autorun') }},
        {{ trx_json_extract('src', ['DDS_SCOPE'], type='varchar', alias='dds_scope') }},
        {{ trx_json_extract('src', ['DDS_ORGANIZATION'], type='varchar', alias='dds_organization') }},
        {{ trx_json_extract('src', ['DDS_UPDATESTAMP'], type='timestamp_ntz', alias='dds_updatestamp') }},
        {{ trx_json_extract('src', ['DDS_UPDATEBYUSER'], type='varchar', alias='dds_updatebyuser') }},
        {{ trx_json_extract('src', ['DDS_CREATEDSTAMP'], type='timestamp_ntz', alias='dds_createdstamp') }},
        {{ trx_json_extract('src', ['DDS_UPDATEABLE'], type='varchar', alias='dds_updateable') }},
        {{ trx_json_extract('src', ['DDS_FILTERSTRXML'], type='varchar', alias='dds_filterstrxml') }},
        {{ trx_json_extract('src', ['DDS_SORTSTRXML'], type='varchar', alias='dds_sortstrxml') }},
        {{ trx_json_extract('src', ['DDS_FIELDLIST'], type='varchar', alias='dds_fieldlist') }},
        {{ trx_json_extract('src', ['DDS_UPDATECOUNT'], type='float', alias='dds_updatecount') }},
        {{ trx_json_extract('src', ['DDS_DISPLAYROW'], type='float', alias='dds_displayrow') }},
        {{ trx_json_extract('src', ['DDS_OWNER'], type='varchar', alias='dds_owner') }},
        {{ trx_json_extract('src', ['DDS_GRIDID'], type='float', alias='dds_gridid') }},
        {{ trx_json_extract('src', ['DDS_TYPE'], type='varchar', alias='dds_type') }},
        {{ trx_json_extract('src', ['DDS_DEFAULTFLAG'], type='varchar', alias='dds_defaultflag') }},
        {{ trx_json_extract('src', ['DDS_FIELDLIST_PORTLET'], type='varchar', alias='dds_fieldlist_portlet') }},
        {{ trx_json_extract('src', ['DDS_CLIENTROWS'], type='float', alias='dds_clientrows') }},
        {{ trx_json_extract('src', ['DDS_PORTLETROWS'], type='float', alias='dds_portletrows') }},
        {{ trx_json_extract('src', ['DDS_HINTS'], type='varchar', alias='dds_hints') }},
        {{ trx_json_extract('src', ['DDS_BOTNAME'], type='varchar', alias='dds_botname') }},
        {{ trx_json_extract('src', ['DDS_USERFILTER'], type='varchar', alias='dds_userfilter') }},
        {{ trx_json_extract('src', ['DDS_SECURITYDATASPY'], type='varchar', alias='dds_securitydataspy') }},
        {{ trx_json_extract('src', ['DDS_MEKEY'], type='varchar', alias='dds_mekey') }},
        {{ trx_json_extract('src', ['DDS_UPDATED'], type='timestamp_ntz', alias='dds_updated') }},
        {{ trx_json_extract('src', ['DDS_DDSPYFILTER'], type='varchar', alias='dds_ddspyfilter') }},
        {{ trx_json_extract('src', ['DDS_GLOBALDATASPY'], type='varchar', alias='dds_globaldataspy') }},
        {{ trx_json_extract('src', ['DDS_BLACKLISTVIOLATIONS'], type='float', alias='dds_blacklistviolations') }},
        {{ trx_json_extract('src', ['DDS_DDSPYNAME'], type='varchar', alias='dds_ddspyname') }},
        {{ trx_json_extract('src', ['DDS_DDSPYID'], type='float', alias='dds_ddspyid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dddataspy') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dds_ddspyid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dds_lastsaved']) }}
