CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_CDR_USE_USESTATUSLOG
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_CDR_USE_USESTATUSLOG')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_CDR_USE_USESTATUSLOG() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_CDR_USE_USESTATUSLOG resume;