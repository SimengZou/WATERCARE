CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_BUDGETING_CATBGTLINK
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_BUDGETING_CATBGTLINK')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_BUDGETING_CATBGTLINK() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_BUDGETING_CATBGTLINK resume;