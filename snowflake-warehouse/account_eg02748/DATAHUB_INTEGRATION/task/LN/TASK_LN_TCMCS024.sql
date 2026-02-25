CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_LN_TCMCS024
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_LN_TCMCS024')
            as 
            call DATAHUB_INTEGRATION.SP_LN_TCMCS024() ;
            alter task DATAHUB_INTEGRATION.TASK_LN_TCMCS024 resume;