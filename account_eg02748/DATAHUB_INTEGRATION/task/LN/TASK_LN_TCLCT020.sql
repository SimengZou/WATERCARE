CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_LN_TCLCT020
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_LN_TCLCT020')
            as 
            call DATAHUB_INTEGRATION.SP_LN_TCLCT020() ;
            alter task DATAHUB_INTEGRATION.TASK_LN_TCLCT020 resume;