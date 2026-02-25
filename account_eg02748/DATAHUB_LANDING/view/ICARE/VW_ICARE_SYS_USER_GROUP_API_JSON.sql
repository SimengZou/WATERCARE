create or replace view VW_ICARE_SYS_USER_GROUP_API_JSON as 

select

value:parent::varchar as "PARENT",

value:manager::varchar as "MANAGER",

TRY_TO_NUMBER(value:sys_mod_count::varchar) as "SYS_MOD_COUNT",

TRY_TO_BOOLEAN(value:active::varchar) as "ACTIVE",

value:description::varchar as "DESCRIPTION",

value:source::varchar as "SOURCE",

TRY_TO_TIMESTAMP_NTZ(value:sys_updated_on::varchar) as "SYS_UPDATED_ON",

value:sys_tags::varchar as "SYS_TAGS",

value:type::varchar as "TYPE",

value:sys_id::varchar as "SYS_ID",

value:sys_updated_by::varchar as "SYS_UPDATED_BY",

value:cost_center::varchar as "COST_CENTER",

value:default_assignee::varchar as "DEFAULT_ASSIGNEE",

TRY_TO_TIMESTAMP_NTZ(value:sys_created_on::varchar) as "SYS_CREATED_ON",

value:name::varchar as "NAME",

value:exclude_manager::varchar as "EXCLUDE_MANAGER",

value:email::varchar as "EMAIL",

value:include_members::varchar as "INCLUDE_MEMBERS",

value:sys_created_by::varchar as "SYS_CREATED_BY",



TRY_TO_NUMBER(value:hourly_rate::varchar) as "HOURLY_RATE",

value:average_daily_fte::varchar as "AVERAGE_DAILY_FTE",

TRY_TO_NUMBER(value:points::varchar) as "POINTS",

value:u_digital_team::varchar as "U_DIGITAL_TEAM"




  from

    DATAHUB_LANDING.ICARE_SYS_USER_GROUP_API 

  , lateral flatten( input => src:result );