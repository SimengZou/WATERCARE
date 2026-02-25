CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_PORTAL_MEMBERSHIP
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_PORTAL_MEMBERSHIP')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_PORTAL_MEMBERSHIP() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_PORTAL_MEMBERSHIP resume;