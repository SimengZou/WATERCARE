create or replace view VW_ICARE_ALL_ILLNESSES_API_JSON as 

select

value:illness_cause::varchar as "ILLNESS_CAUSE",

TRY_TO_NUMBER(value:sys_mod_count::varchar) as "SYS_MOD_COUNT",

value:illness_reason::varchar as "ILLNESS_REASON",

TRY_TO_TIMESTAMP_NTZ(value:sys_updated_on::varchar) as "SYS_UPDATED_ON",

value:sys_tags::varchar as "SYS_TAGS",

value:icare_incident::varchar as "ICARE_INCIDENT",

value:sys_id::varchar as "SYS_ID",

value:sys_updated_by::varchar as "SYS_UPDATED_BY",

value:illness_type::varchar as "ILLNESS_TYPE",

TRY_TO_TIMESTAMP_NTZ(value:sys_created_on::varchar) as "SYS_CREATED_ON",

value:external_user::varchar as "EXTERNAL_USER",

value:user::varchar as "USER",

value:sys_created_by::varchar as "SYS_CREATED_BY"
--,SYSDATE() as ETL_LOAD_DATETIME


  from

    DATAHUB_LANDING.ICARE_ALL_ILLNESSES_API

  , lateral flatten( input => src:result );