CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_LN_WHINA115
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_LN_WHINA115')
            as 
            call DATAHUB_INTEGRATION.SP_LN_WHINA115() ;
            alter task DATAHUB_INTEGRATION.TASK_LN_WHINA115 resume;