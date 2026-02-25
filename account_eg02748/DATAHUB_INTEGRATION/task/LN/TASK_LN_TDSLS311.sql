CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_LN_TDSLS311
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_LN_TDSLS311')
            as 
            call DATAHUB_INTEGRATION.SP_LN_TDSLS311() ;
            alter task DATAHUB_INTEGRATION.TASK_LN_TDSLS311 resume;