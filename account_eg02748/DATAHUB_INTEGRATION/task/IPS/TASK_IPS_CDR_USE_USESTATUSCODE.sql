CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_CDR_USE_USESTATUSCODE
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_CDR_USE_USESTATUSCODE')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_CDR_USE_USESTATUSCODE() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_CDR_USE_USESTATUSCODE resume;