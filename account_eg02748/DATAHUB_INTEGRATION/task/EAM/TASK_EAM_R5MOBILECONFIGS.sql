CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_EAM_R5MOBILECONFIGS
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_EAM_R5MOBILECONFIGS')
            as 
            call DATAHUB_INTEGRATION.SP_EAM_R5MOBILECONFIGS() ;
            alter task DATAHUB_INTEGRATION.TASK_EAM_R5MOBILECONFIGS resume;