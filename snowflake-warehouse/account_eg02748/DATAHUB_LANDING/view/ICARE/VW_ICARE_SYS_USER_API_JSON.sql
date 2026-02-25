create or replace view VW_ICARE_SYS_USER_API_JSON as 

select

value:calendar_integration::varchar as "CALENDAR_INTEGRATION",

value:country::varchar as "COUNTRY",

value:u_wap::varchar as "U_WAP",

value:u_group::varchar as "U_GROUP",

value:last_login_time::varchar as "LAST_LOGIN_TIME",

value:source::varchar as "SOURCE",

TRY_TO_TIMESTAMP_NTZ(value:sys_updated_on::varchar) as "SYS_UPDATED_ON",

value:building::varchar as "BUILDING",

value:web_service_access_only::varchar as "WEB_SERVICE_ACCESS_ONLY",

value:notification::varchar as "NOTIFICATION",

value:enable_multifactor_authn::varchar as "ENABLE_MULTIFACTOR_AUTHN",

value:sys_updated_by::varchar as "SYS_UPDATED_BY",

value:sso_source::varchar as "SSO_SOURCE",

TRY_TO_TIMESTAMP_NTZ(value:sys_created_on::varchar) as "SYS_CREATED_ON",

value:sys_domain::varchar as "SYS_DOMAIN",

value:state::varchar as "STATE",

value:vip::varchar as "VIP",

value:sys_created_by::varchar as "SYS_CREATED_BY",

value:u_mgrfororg::varchar as "U_MGRFORORG",

value:zip::varchar as "ZIP",

value:home_phone::varchar as "HOME_PHONE",

value:time_format::varchar as "TIME_FORMAT",

value:last_login::varchar as "LAST_LOGIN",

value:u_wap_vacant::varchar as "U_WAP_VACANT",

TRY_TO_BOOLEAN(value:active::varchar) as "ACTIVE",

value:sys_domain_path::varchar as "SYS_DOMAIN_PATH",

value:transaction_log::varchar as "TRANSACTION_LOG",

value:cost_center::varchar as "COST_CENTER",

value:phone::varchar as "PHONE",

value:name::varchar as "NAME",

value:employee_number::varchar as "EMPLOYEE_NUMBER",

value:gender::varchar as "GENDER",

value:city::varchar as "CITY",

value:failed_attempts::varchar as "FAILED_ATTEMPTS",

value:user_name::varchar as "USER_NAME",

value:u_mgrforapprovals::varchar as "U_MGRFORAPPROVALS",

value:title::varchar as "TITLE",

value:sys_class_name::varchar as "SYS_CLASS_NAME",

value:sys_id::varchar as "SYS_ID",

value:u_manager_s_wap_derived::varchar as "U_MANAGER_S_WAP_DERIVED",

value:internal_integration_user::varchar as "INTERNAL_INTEGRATION_USER",

value:mobile_phone::varchar as "MOBILE_PHONE",

value:street::varchar as "STREET",

value:department::varchar as "DEPARTMENT",

value:sn_hr_ef_purging_stride::varchar as "SN_HR_EF_PURGING_STRIDE",

value:first_name::varchar as "FIRST_NAME",

value:email::varchar as "EMAIL",

value:introduction::varchar as "INTRODUCTION",

value:preferred_language::varchar as "PREFERRED_LANGUAGE",

value:u_number::varchar as "U_NUMBER",

value:u_mgrwap::varchar as "U_MGRWAP",

value:u_legal_first_name::varchar as "U_LEGAL_FIRST_NAME",

value:manager::varchar as "MANAGER",

value:u_code::varchar as "U_CODE",

TRY_TO_NUMBER(value:sys_mod_count::varchar) as "SYS_MOD_COUNT",

value:last_name::varchar as "LAST_NAME",

value:photo::varchar as "PHOTO",

value:avatar::varchar as "AVATAR",

value:u_chief_wap::varchar as "U_CHIEF_WAP",

value:middle_name::varchar as "MIDDLE_NAME",

value:sys_tags::varchar as "SYS_TAGS",

value:time_zone::varchar as "TIME_ZONE",

value:schedule::varchar as "SCHEDULE",

value:correlation_id::varchar as "CORRELATION_ID",

value:date_format::varchar as "DATE_FORMAT",

value:location::varchar as "LOCATION",

value:u_business_unit::varchar as "U_BUSINESS_UNIT",

value:average_daily_fte::varchar as "AVERAGE_DAILY_FTE"
--,SYSDATE() as ETL_LOAD_DATETIME
  from

    DATAHUB_LANDING.ICARE_SYS_USER_API 

  , lateral flatten( input => src:result );