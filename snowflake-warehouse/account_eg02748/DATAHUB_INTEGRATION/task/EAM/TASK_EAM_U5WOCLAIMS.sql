CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_EAM_U5WOCLAIMS
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_EAM_U5WOCLAIMS')
            as 
            call DATAHUB_INTEGRATION.SP_EAM_U5WOCLAIMS() ;
            alter task DATAHUB_INTEGRATION.TASK_EAM_U5WOCLAIMS resume;