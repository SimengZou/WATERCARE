create or replace view VW_ICARE_CASE_FEEDBACK_API_JSON as 

select

value:parent::varchar as "PARENT",

TRY_TO_BOOLEAN(value:made_sla::varchar) as "MADE_SLA",

value:event_date_time::varchar as "EVENT_DATE_TIME",

value:event_location::varchar as "EVENT_LOCATION",

value:watch_list::varchar as "WATCH_LIST",

value:recommendation::varchar as "RECOMMENDATION",

value:sn_esign_document::varchar as "SN_ESIGN_DOCUMENT",

value:upon_reject::varchar as "UPON_REJECT",

value:requested_for::varchar as "REQUESTED_FOR",

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

value:report_for::varchar as "REPORT_FOR",

value:route_reason::varchar as "ROUTE_REASON",

value:sys_created_by::varchar as "SYS_CREATED_BY",

TRY_TO_BOOLEAN(value:knowledge::varchar) as "KNOWLEDGE",

value:order::varchar as "ORDER",

TRY_TO_TIMESTAMP_NTZ(value:closed_at::varchar) as "CLOSED_AT",

value:cmdb_ci::varchar as "CMDB_CI",

value:contract::varchar as "CONTRACT",

value:impact::varchar as "IMPACT",

TRY_TO_BOOLEAN(value:active::varchar) as "ACTIVE",

value:requested_for_hr_profile::varchar as "REQUESTED_FOR_HR_PROFILE",

value:business_service::varchar as "BUSINESS_SERVICE",

value:priority::varchar as "PRIORITY",

value:sys_domain_path::varchar as "SYS_DOMAIN_PATH",

TRY_TO_NUMBER(value:time_worked::varchar) as "TIME_WORKED",

TRY_TO_TIMESTAMP_NTZ(value:expected_start::varchar) as "EXPECTED_START",

TRY_TO_TIMESTAMP_NTZ(value:opened_at::varchar) as "OPENED_AT",

TRY_TO_NUMBER(value:business_duration::varchar) as "BUSINESS_DURATION",

value:group_list::varchar as "GROUP_LIST",

TRY_TO_TIMESTAMP_NTZ(value:work_end::varchar) as "WORK_END",

value:visible_feedback::varchar as "VISIBLE_FEEDBACK",

value:event_location_details::varchar as "EVENT_LOCATION_DETAILS",

value:universal_request::varchar as "UNIVERSAL_REQUEST",

value:short_description::varchar as "SHORT_DESCRIPTION",

value:correlation_display::varchar as "CORRELATION_DISPLAY",

value:work_start::varchar as "WORK_START",

value:assignment_group::varchar as "ASSIGNMENT_GROUP",

value:additional_assignee_list::varchar as "ADDITIONAL_ASSIGNEE_LIST",

value:description::varchar as "DESCRIPTION",

value:event_address::varchar as "EVENT_ADDRESS",

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

value:worksafe_notified::varchar as "WORKSAFE_NOTIFIED",

value:comments::varchar as "COMMENTS",

value:approval::varchar as "APPROVAL",

value:sla_due::varchar as "SLA_DUE",

value:work_related::varchar as "WORK_RELATED",

TRY_TO_TIMESTAMP_NTZ(value:due_date::varchar) as "DUE_DATE",

TRY_TO_NUMBER(value:sys_mod_count::varchar) as "SYS_MOD_COUNT",

value:sys_tags::varchar as "SYS_TAGS",

value:old_icare_reference::varchar as "OLD_ICARE_REFERENCE",

value:contact_number::varchar as "CONTACT_NUMBER",

value:escalation::varchar as "ESCALATION",

value:upon_approval::varchar as "UPON_APPROVAL",

value:what_you_saw::varchar as "WHAT_YOU_SAW",

value:correlation_id::varchar as "CORRELATION_ID",

value:location::varchar as "LOCATION",

value:u_business_unit::varchar as "U_BUSINESS_UNIT",

value:feedback_type::varchar as "FEEDBACK_TYPE",

value:reporter_type::varchar as "REPORTER_TYPE",

value:contractor_company_name::varchar as "CONTRACTOR_COMPANY_NAME",



value:work_notes::varchar as "WORK_NOTES",

value:approval_set::varchar as "APPROVAL_SET",

value:sn_hr_le_activity::varchar as "SN_HR_LE_ACTIVITY",

value:company::varchar as "COMPANY",

value:service_offering::varchar as "SERVICE_OFFERING",

value:delivery_plan::varchar as "DELIVERY_PLAN",

value:delivery_task::varchar as "DELIVERY_TASK",

value:work_notes_list::varchar as "WORK_NOTES_LIST",

value:comments_and_work_notes::varchar as "COMMENTS_AND_WORK_NOTES",

value:work_type::varchar as "WORK_TYPE"

--,SYSDATE() as ETL_LOAD_DATETIME
  from

    DATAHUB_LANDING.ICARE_CASE_FEEDBACK_API

  , lateral flatten( input => src:result );