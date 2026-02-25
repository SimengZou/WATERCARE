create or replace view VW_ICARE_SN_HR_CORE_PROFILE_RESPONSE_API_JSON as 

select

value:number::varchar as "NUMBER",

value:sys_id::varchar as "SYS_ID",

value:sys_domain::varchar as "SYS_DOMAIN",

value:work_mobile::varchar as "WORK_MOBILE",

value:work_phone::varchar as "WORK_PHONE",

value:employee_number::varchar as "EMPLOYEE_NUMBER",

value:location::varchar as "LOCATION",

value:user::varchar as "USER"
--,SYSDATE() as ETL_LOAD_DATETIME

  from

    DATAHUB_LANDING.ICARE_SN_HR_CORE_PROFILE_RESPONSE_API

  , lateral flatten( input => src:result );