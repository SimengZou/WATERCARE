create or replace view VW_ICARE_CASE_GENERAL_WORKPLACE_INSPECTION_API_JSON as 

select

value:parent::varchar as "PARENT",

value:are_first_aid_kits_stocked::varchar as "ARE_FIRST_AID_KITS_STOCKED",

value:watch_list::varchar as "WATCH_LIST",

value:has_hsw_meeting_occurred::varchar as "HAS_HSW_MEETING_OCCURRED",

value:upon_reject::varchar as "UPON_REJECT",

value:requested_for::varchar as "REQUESTED_FOR",

TRY_TO_TIMESTAMP_NTZ(value:sys_updated_on::varchar) as "SYS_UPDATED_ON",

value:walkways_comments::varchar as "WALKWAYS_COMMENTS",

value:approval_history::varchar as "APPROVAL_HISTORY",

value:feedback::varchar as "FEEDBACK",

value:skills::varchar as "SKILLS",

value:are_flotation_devices_adequate::varchar as "ARE_FLOTATION_DEVICES_ADEQUATE",

value:number::varchar as "NUMBER",

value:are_gas_cylinders_secured::varchar as "ARE_GAS_CYLINDERS_SECURED",

value:state::varchar as "STATE",

value:report_for::varchar as "REPORT_FOR",

value:sys_created_by::varchar as "SYS_CREATED_BY",

TRY_TO_BOOLEAN(value:knowledge::varchar) as "KNOWLEDGE",

value:order::varchar as "ORDER",

value:are_emerg_procedures_valid::varchar as "ARE_EMERG_PROCEDURES_VALID",

value:is_entrance_cleared::varchar as "IS_ENTRANCE_CLEARED",

value:cmdb_ci::varchar as "CMDB_CI",

value:contract::varchar as "CONTRACT",

value:impact::varchar as "IMPACT",

TRY_TO_BOOLEAN(value:active::varchar) as "ACTIVE",

value:requested_for_hr_profile::varchar as "REQUESTED_FOR_HR_PROFILE",

value:are_stairs_in_good_condition::varchar as "ARE_STAIRS_IN_GOOD_CONDITION",

value:priority::varchar as "PRIORITY",

value:sys_domain_path::varchar as "SYS_DOMAIN_PATH",

value:are_entrances_marked::varchar as "ARE_ENTRANCES_MARKED",

TRY_TO_NUMBER(value:business_duration::varchar) as "BUSINESS_DURATION",

value:group_list::varchar as "GROUP_LIST",

value:are_wash_stations_adequate::varchar as "ARE_WASH_STATIONS_ADEQUATE",

value:event_location_details::varchar as "EVENT_LOCATION_DETAILS",

value:universal_request::varchar as "UNIVERSAL_REQUEST",

value:clean_tidy_comments::varchar as "CLEAN_TIDY_COMMENTS",

value:short_description::varchar as "SHORT_DESCRIPTION",

value:correlation_display::varchar as "CORRELATION_DISPLAY",

value:work_start::varchar as "WORK_START",

value:additional_assignee_list::varchar as "ADDITIONAL_ASSIGNEE_LIST",

value:are_chems_labelled::varchar as "ARE_CHEMS_LABELLED",

value:sys_class_name::varchar as "SYS_CLASS_NAME",

value:closed_by::varchar as "CLOSED_BY",

value:follow_up::varchar as "FOLLOW_UP",

value:are_floors_cleared::varchar as "ARE_FLOORS_CLEARED",

value:are_appliances_tested::varchar as "ARE_APPLIANCES_TESTED",

TRY_TO_NUMBER(value:reassignment_count::varchar) as "REASSIGNMENT_COUNT",

value:assigned_to::varchar as "ASSIGNED_TO",

value:worksafe_notified::varchar as "WORKSAFE_NOTIFIED",

value:are_chem_bunds_empty::varchar as "ARE_CHEM_BUNDS_EMPTY",

value:sla_due::varchar as "SLA_DUE",

value:work_related::varchar as "WORK_RELATED",

value:hazard_comments::varchar as "HAZARD_COMMENTS",

value:escalation::varchar as "ESCALATION",

value:upon_approval::varchar as "UPON_APPROVAL",

value:storage_comments::varchar as "STORAGE_COMMENTS",

value:are_waste_bins_adequate::varchar as "ARE_WASTE_BINS_ADEQUATE",

value:correlation_id::varchar as "CORRELATION_ID",

value:u_business_unit::varchar as "U_BUSINESS_UNIT",

value:reporter_type::varchar as "REPORTER_TYPE",

value:contractor_company_name::varchar as "CONTRACTOR_COMPANY_NAME",

value:no_excessive_loading::varchar as "NO_EXCESSIVE_LOADING",

TRY_TO_BOOLEAN(value:made_sla::varchar) as "MADE_SLA",

value:event_date_time::varchar as "EVENT_DATE_TIME",

value:event_location::varchar as "EVENT_LOCATION",

value:is_sds_available::varchar as "IS_SDS_AVAILABLE",

value:sn_esign_document::varchar as "SN_ESIGN_DOCUMENT",

value:task_effective_number::varchar as "TASK_EFFECTIVE_NUMBER",

value:are_leads_plugs_undamaged::varchar as "ARE_LEADS_PLUGS_UNDAMAGED",

value:hsw_comments::varchar as "HSW_COMMENTS",

value:are_hose_reels_available::varchar as "ARE_HOSE_REELS_AVAILABLE",

value:sys_updated_by::varchar as "SYS_UPDATED_BY",

value:are_chem_containers_adequate::varchar as "ARE_CHEM_CONTAINERS_ADEQUATE",

value:opened_by::varchar as "OPENED_BY",

value:user_input::varchar as "USER_INPUT",

TRY_TO_TIMESTAMP_NTZ(value:sys_created_on::varchar) as "SYS_CREATED_ON",

value:sys_domain::varchar as "SYS_DOMAIN",

value:are_materials_stored::varchar as "ARE_MATERIALS_STORED",

value:route_reason::varchar as "ROUTE_REASON",

TRY_TO_TIMESTAMP_NTZ(value:closed_at::varchar) as "CLOSED_AT",

value:is_emerg_equip_available::varchar as "IS_EMERG_EQUIP_AVAILABLE",

value:is_contractor_inducted::varchar as "IS_CONTRACTOR_INDUCTED",

value:business_service::varchar as "BUSINESS_SERVICE",

value:is_shelving_in_good_condition::varchar as "IS_SHELVING_IN_GOOD_CONDITION",

value:stairs_comments::varchar as "STAIRS_COMMENTS",

value:lighting_comments::varchar as "LIGHTING_COMMENTS",

TRY_TO_NUMBER(value:time_worked::varchar) as "TIME_WORKED",

TRY_TO_TIMESTAMP_NTZ(value:expected_start::varchar) as "EXPECTED_START",

TRY_TO_TIMESTAMP_NTZ(value:opened_at::varchar) as "OPENED_AT",

value:are_handrails_fitted::varchar as "ARE_HANDRAILS_FITTED",

TRY_TO_TIMESTAMP_NTZ(value:work_end::varchar) as "WORK_END",

value:electrical_comments::varchar as "ELECTRICAL_COMMENTS",

value:are_landings_clear::varchar as "ARE_LANDINGS_CLEAR",

value:assignment_group::varchar as "ASSIGNMENT_GROUP",

value:are_chems_separated::varchar as "ARE_CHEMS_SEPARATED",

value:description::varchar as "DESCRIPTION",

value:event_address::varchar as "EVENT_ADDRESS",

value:u_html_description::varchar as "U_HTML_DESCRIPTION",

TRY_TO_NUMBER(value:calendar_duration::varchar) as "CALENDAR_DURATION",

value:are_rungs_bolts_secure::varchar as "ARE_RUNGS_BOLTS_SECURE",

value:close_notes::varchar as "CLOSE_NOTES",

value:sys_id::varchar as "SYS_ID",

value:contact_type::varchar as "CONTACT_TYPE",

value:sn_esign_esignature_configuration::varchar as "SN_ESIGN_ESIGNATURE_CONFIGURATION",

value:is_emerg_signage_visible::varchar as "IS_EMERG_SIGNAGE_VISIBLE",

value:is_int_ext_lighting_adequate::varchar as "IS_INT_EXT_LIGHTING_ADEQUATE",

value:urgency::varchar as "URGENCY",

value:is_no_lead_piggy_backing::varchar as "IS_NO_LEAD_PIGGY_BACKING",

value:activity_due::varchar as "ACTIVITY_DUE",

value:comments::varchar as "COMMENTS",

value:is_lighting_adequate::varchar as "IS_LIGHTING_ADEQUATE",

value:approval::varchar as "APPROVAL",

value:are_chem_containers_labelled::varchar as "ARE_CHEM_CONTAINERS_LABELLED",

TRY_TO_TIMESTAMP_NTZ(value:due_date::varchar) as "DUE_DATE",

value:emergency_comments::varchar as "EMERGENCY_COMMENTS",

TRY_TO_NUMBER(value:sys_mod_count::varchar) as "SYS_MOD_COUNT",

value:is_spill_kit_available::varchar as "IS_SPILL_KIT_AVAILABLE",

value:sys_tags::varchar as "SYS_TAGS",

value:old_icare_reference::varchar as "OLD_ICARE_REFERENCE",

value:contact_number::varchar as "CONTACT_NUMBER",

value:is_chem_signage_visible::varchar as "IS_CHEM_SIGNAGE_VISIBLE",

value:are_team_areas_clean::varchar as "ARE_TEAM_AREAS_CLEAN",

value:location::varchar as "LOCATION",



value:work_notes::varchar as "WORK_NOTES",

value:approval_set::varchar as "APPROVAL_SET",

value:sn_hr_le_activity::varchar as "SN_HR_LE_ACTIVITY",

value:company::varchar as "COMPANY",

value:service_offering::varchar as "SERVICE_OFFERING",

value:delivery_plan::varchar as "DELIVERY_PLAN",

value:delivery_task::varchar as "DELIVERY_TASK",

value:work_notes_list::varchar as "WORK_NOTES_LIST",

value:comments_and_work_notes::varchar as "COMMENTS_AND_WORK_NOTES"
--,SYSDATE() as ETL_LOAD_DATETIME


  from

    DATAHUB_LANDING.ICARE_CASE_GENERAL_WORKPLACE_INSPECTION_API

  , lateral flatten( input => src:result );