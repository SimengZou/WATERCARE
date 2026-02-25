CREATE OR REPLACE PROCEDURE "ICARE_TRUNCATE_ALL_TABLES"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
var result = "";
snowflake.execute( {sqlText: "begin transaction;"} );
try {

snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_ALL_ICARE_ACTIONS_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_ALL_ICARE_CASES_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_ALL_ILLNESSES_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_ALL_INJURIES_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_CLOSE_CALL_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_GENERAL_WORKPLACE_INSPECTION_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_HAZARD_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_MEDICAL_APPOINTMENT_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_PAIN_OR_DISCOMFORT_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_PERMIT_AUDIT_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_SITE_WALK_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_WELLBEING_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_WORKSAFE_ENGAGEMENT_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_EVENT_LOCATION_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_INCIDENT_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_SN_HR_CORE_PROFILE_RESPONSE_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_SYS_USER_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_SYS_USER_GROUP_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_TASK_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_CONTRACTOR_STATISTIC;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_EMERGENCY_DRILL_API;
;`
} );
snowflake.execute( {sqlText:
` TRUNCATE TABLE TARGET_ICARE.ICARE_CASE_FEEDBACK_API;
;`
} );

snowflake.execute( {sqlText: "commit;"} );
result = "Succeeded";
}
catch (err) {
snowflake.execute( {sqlText: "rollback;"} );
return "Failed: " + err; // Return a success/error indicator.
}
return result;
';