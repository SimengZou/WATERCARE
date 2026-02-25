create or replace view VW_ICARE_EVENT_LOCATION_API_JSON as 

select

value:address::varchar as "ADDRESS",

value:hsw_business_partner::varchar as "HSW_BUSINESS_PARTNER",

value:watch_list::varchar as "WATCH_LIST",

TRY_TO_NUMBER(value:sys_mod_count::varchar) as "SYS_MOD_COUNT",

value:mandatory_address::varchar as "MANDATORY_ADDRESS",

TRY_TO_TIMESTAMP_NTZ(value:sys_updated_on::varchar) as "SYS_UPDATED_ON",

value:sys_tags::varchar as "SYS_TAGS",

value:sys_id::varchar as "SYS_ID",

value:sys_updated_by::varchar as "SYS_UPDATED_BY",

value:location_owners::varchar as "LOCATION_OWNERS",

TRY_TO_TIMESTAMP_NTZ(value:sys_created_on::varchar) as "SYS_CREATED_ON",

value:name::varchar as "NAME",

value:parent_location::varchar as "PARENT_LOCATION",

value:sys_created_by::varchar as "SYS_CREATED_BY"

--,SYSDATE() as ETL_LOAD_DATETIME

  from

    DATAHUB_LANDING.ICARE_EVENT_LOCATION_API

  , lateral flatten( input => src:result );