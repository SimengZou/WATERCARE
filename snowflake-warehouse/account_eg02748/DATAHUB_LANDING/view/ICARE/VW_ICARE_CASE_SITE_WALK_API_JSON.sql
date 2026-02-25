create or replace view VW_ICARE_CASE_SITE_WALK_API_JSON
 as 

select

value:parent::varchar as "PARENT",

TRY_TO_BOOLEAN(value:made_sla::varchar) as "MADE_SLA",

value:do_hazards_risks_match::varchar as "DO_HAZARDS_RISKS_MATCH",

value:event_date_time::varchar as "EVENT_DATE_TIME",

value:event_location::varchar as "EVENT_LOCATION",

value:watch_list::varchar as "WATCH_LIST",

value:are_materials_stacked_stable::varchar as "ARE_MATERIALS_STACKED_STABLE",

value:sn_esign_document::varchar as "SN_ESIGN_DOCUMENT",

value:upon_reject::varchar as "UPON_REJECT",

value:requested_for::varchar as "REQUESTED_FOR",

TRY_TO_TIMESTAMP_NTZ(value:sys_updated_on::varchar) as "SYS_UPDATED_ON",

value:task_effective_number::varchar as "TASK_EFFECTIVE_NUMBER",

value:approval_history::varchar as "APPROVAL_HISTORY",

value:feedback::varchar as "FEEDBACK",

value:skills::varchar as "SKILLS",

value:are_works_clear_of_equipment::varchar as "ARE_WORKS_CLEAR_OF_EQUIPMENT",

value:number::varchar as "NUMBER",

value:sys_updated_by::varchar as "SYS_UPDATED_BY",

value:opened_by::varchar as "OPENED_BY",

value:user_input::varchar as "USER_INPUT",

TRY_TO_TIMESTAMP_NTZ(value:sys_created_on::varchar) as "SYS_CREATED_ON",

value:sys_domain::varchar as "SYS_DOMAIN",

value:is_ppe_worn::varchar as "IS_PPE_WORN",

value:state::varchar as "STATE",

value:report_for::varchar as "REPORT_FOR",

value:is_rubbish_stored_correctly::varchar as "IS_RUBBISH_STORED_CORRECTLY",

value:route_reason::varchar as "ROUTE_REASON",

value:sys_created_by::varchar as "SYS_CREATED_BY",

TRY_TO_BOOLEAN(value:knowledge::varchar) as "KNOWLEDGE",

value:order::varchar as "ORDER",

TRY_TO_TIMESTAMP_NTZ(value:closed_at::varchar) as "CLOSED_AT",

value:cmdb_ci::varchar as "CMDB_CI",

value:is_access_required::varchar as "IS_ACCESS_REQUIRED",

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

value:are_controls_in_place::varchar as "ARE_CONTROLS_IN_PLACE",

TRY_TO_NUMBER(value:business_duration::varchar) as "BUSINESS_DURATION",

value:group_list::varchar as "GROUP_LIST",

value:are_amenities_maintained::varchar as "ARE_AMENITIES_MAINTAINED",

TRY_TO_TIMESTAMP_NTZ(value:work_end::varchar) as "WORK_END",

value:equipment_condition::varchar as "EQUIPMENT_CONDITION",

value:event_location_details::varchar as "EVENT_LOCATION_DETAILS",

value:universal_request::varchar as "UNIVERSAL_REQUEST",

value:short_description::varchar as "SHORT_DESCRIPTION",

value:correlation_display::varchar as "CORRELATION_DISPLAY",

value:work_start::varchar as "WORK_START",

value:assignment_group::varchar as "ASSIGNMENT_GROUP",

value:operating_safely::varchar as "OPERATING_SAFELY",

value:is_area_free_of_rubbish::varchar as "IS_AREA_FREE_OF_RUBBISH",

value:additional_assignee_list::varchar as "ADDITIONAL_ASSIGNEE_LIST",

value:description::varchar as "DESCRIPTION",

value:event_address::varchar as "EVENT_ADDRESS",

value:u_html_description::varchar as "U_HTML_DESCRIPTION",

value:is_toolbox_meeting_completed::varchar as "IS_TOOLBOX_MEETING_COMPLETED",

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

value:correlation_id::varchar as "CORRELATION_ID",

value:location::varchar as "LOCATION",

value:delineated_for_parking::varchar as "DELINEATED_FOR_PARKING",

value:u_business_unit::varchar as "U_BUSINESS_UNIT",

value:is_physical_edge_protection::varchar as "IS_PHYSICAL_EDGE_PROTECTION",

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
value:walkway_clearly_marked_and_free_from_obstructions::varchar as walkway_clearly_marked_and_free_from_obstructions,
value:condition_of_entry_of_site_clearly_displayed::varchar as condition_of_entry_of_site_clearly_displayed,
value:jsa_s_relevant_to_the_task_and_are_the_control_measures_been_implement::varchar as jsa_s_relevant_to_the_task_and_are_the_control_measures_been_implement,
value:emergency_equipment_available::varchar as emergency_equipment_available,
value:comment_on_permit_to_work::varchar as comment_on_permit_to_work,
value:review_1_site_registers::varchar as review_1_site_registers,
value:comment_on_site_walk::varchar as comment_on_site_walk,
value:hazard_board_up_to_date::varchar as hazard_board_up_to_date,
value:ladder_on_site_are_az_nzs_1892_1966::varchar as ladder_on_site_are_az_nzs_1892_1966,
value:approved_traffic_management_plan_in_place::varchar as approved_traffic_management_plan_in_place,
value:working_at_height_equipment_in_current_test::varchar as working_at_height_equipment_in_current_test,
value:barricading_in_place_to_prevent_fall_from_height::varchar as barricading_in_place_to_prevent_fall_from_height,
value:comment_on_site_registers::varchar as comment_on_site_registers,
value:site_contact_details_displayed_up_to_date::varchar as site_contact_details_displayed_up_to_date,
value:trained_spotters_being_used_for_operational_plant::varchar as trained_spotters_being_used_for_operational_plant,
value:certified_trench_shielding_and_benching_in_place_for_trenches_deeper_than_1_5m::varchar as certified_trench_shielding_and_benching_in_place_for_trenches_deeper_than_1_5m,
value:emergency_response_plans_displayed::varchar as emergency_response_plans_displayed,
value:all_hazardous_substances_stored_correctly::varchar as all_hazardous_substances_stored_correctly,
value:review_1_site_ptw_activities::varchar as review_1_site_ptw_activities,
value:comment_on_site_access::varchar as comment_on_site_access,
value:toilets_available_on_site::varchar as toilets_available_on_site,
value:site_sign_in_process::varchar as site_sign_in_process,
value:scaffolding_have_safe_scaf_tag_and_inspected::varchar as scaffolding_have_safe_scaf_tag_and_inspected,
value:exclusion_zones_in_place::varchar as exclusion_zones_in_place,
value:environmental_controls_in_place_and_working::varchar as environmental_controls_in_place_and_working,
value:pre_start_checks_on_operational_plant::varchar as pre_start_checks_on_operational_plant,
value:all_lifting_equipments_have_tags_attached::varchar as all_lifting_equipments_have_tags_attached,
value:all_overhead_underground_services_identified::varchar as all_overhead_underground_services_identified,
value:electrical_equipments_tagged_and_in_current_test::varchar as  electrical_equipments_tagged_and_in_current_test,
value:jsa_s_available_for_the_active_tasks::varchar as jsa_s_available_for_the_active_tasks
--,SYSDATE() as ETL_LOAD_DATETIME


  from

    DATAHUB_LANDING.ICARE_CASE_SITE_WALK_API

  , lateral flatten( input => src:result );