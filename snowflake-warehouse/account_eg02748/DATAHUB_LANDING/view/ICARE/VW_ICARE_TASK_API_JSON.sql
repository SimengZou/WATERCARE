create or replace view VW_ICARE_TASK_API_JSON as 

select

value:parent::varchar as "PARENT",

TRY_TO_BOOLEAN(value:made_sla::varchar) as "MADE_SLA",

value:watch_list::varchar as "WATCH_LIST",

value:sn_esign_document::varchar as "SN_ESIGN_DOCUMENT",

value:upon_reject::varchar as "UPON_REJECT",

TRY_TO_TIMESTAMP_NTZ(value:sys_updated_on::varchar) as "SYS_UPDATED_ON",

value:task_effective_number::varchar as "TASK_EFFECTIVE_NUMBER",

value:approval_history::varchar as "APPROVAL_HISTORY",

value:skills::varchar as "SKILLS",

value:number::varchar as "NUMBER",

value:sys_updated_by::varchar as "SYS_UPDATED_BY",

value:opened_by::varchar as "OPENED_BY",

value:user_input::varchar as "USER_INPUT",

TRY_TO_TIMESTAMP_NTZ(value:sys_created_on::varchar) as "SYS_CREATED_ON",

value:sys_domain::varchar as "SYS_DOMAIN",

value:state::varchar as "STATE",

value:route_reason::varchar as "ROUTE_REASON",

value:sys_created_by::varchar as "SYS_CREATED_BY",

TRY_TO_BOOLEAN(value:knowledge::varchar) as "KNOWLEDGE",

value:order::varchar as "ORDER",

TRY_TO_TIMESTAMP_NTZ(value:closed_at::varchar) as "CLOSED_AT",

value:cmdb_ci::varchar as "CMDB_CI",

value:contract::varchar as "CONTRACT",

value:impact::varchar as "IMPACT",

TRY_TO_BOOLEAN(value:active::varchar) as "ACTIVE",

value:business_service::varchar as "BUSINESS_SERVICE",

value:priority::varchar as "PRIORITY",

value:sys_domain_path::varchar as "SYS_DOMAIN_PATH",

TRY_TO_NUMBER(value:time_worked::varchar) as "TIME_WORKED",

TRY_TO_TIMESTAMP_NTZ(value:expected_start::varchar) as "EXPECTED_START",

TRY_TO_TIMESTAMP_NTZ(value:opened_at::varchar) as "OPENED_AT",

TRY_TO_NUMBER(value:business_duration::varchar) as "BUSINESS_DURATION",

value:group_list::varchar as "GROUP_LIST",

TRY_TO_TIMESTAMP_NTZ(value:work_end::varchar) as "WORK_END",

value:universal_request::varchar as "UNIVERSAL_REQUEST",

value:short_description::varchar as "SHORT_DESCRIPTION",

value:correlation_display::varchar as "CORRELATION_DISPLAY",

value:work_start::varchar as "WORK_START",

value:assignment_group::varchar as "ASSIGNMENT_GROUP",

value:additional_assignee_list::varchar as "ADDITIONAL_ASSIGNEE_LIST",

value:description::varchar as "DESCRIPTION",

value:u_html_description::varchar as "U_HTML_DESCRIPTION",

TRY_TO_NUMBER(value:calendar_duration::varchar) as "CALENDAR_DURATION",

value:close_notes::varchar as "CLOSE_NOTES",

value:sys_class_name::varchar as "SYS_CLASS_NAME",

value:closed_by::varchar as "CLOSED_BY",

value:follow_up::varchar as "FOLLOW_UP",

value:sys_id::varchar as "SYS_ID",

value:contact_type::varchar as "CONTACT_TYPE",

value:sn_esign_esignature_configuration::varchar as "SN_ESIGN_ESIGNATURE_CONFIGURATION",

value:urgency::varchar as "URGENCY",

TRY_TO_NUMBER(value:reassignment_count::varchar) as "REASSIGNMENT_COUNT",

value:activity_due::varchar as "ACTIVITY_DUE",

value:assigned_to::varchar as "ASSIGNED_TO",

value:comments::varchar as "COMMENTS",

value:approval::varchar as "APPROVAL",

value:sla_due::varchar as "SLA_DUE",

TRY_TO_TIMESTAMP_NTZ(value:due_date::varchar) as "DUE_DATE",

TRY_TO_NUMBER(value:sys_mod_count::varchar) as "SYS_MOD_COUNT",

value:sys_tags::varchar as "SYS_TAGS",

value:escalation::varchar as "ESCALATION",

value:upon_approval::varchar as "UPON_APPROVAL",

value:correlation_id::varchar as "CORRELATION_ID",

value:location::varchar as "LOCATION",

value:u_business_unit::varchar as "U_BUSINESS_UNIT"
--,SYSDATE() as ETL_LOAD_DATETIME


  from

    DATAHUB_LANDING.ICARE_TASK_API 

  , lateral flatten( input => src:result );