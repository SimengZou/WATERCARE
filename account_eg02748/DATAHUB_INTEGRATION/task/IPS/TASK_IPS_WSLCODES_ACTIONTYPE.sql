CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_WSLCODES_ACTIONTYPE
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_WSLCODES_ACTIONTYPE')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_WSLCODES_ACTIONTYPE() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_WSLCODES_ACTIONTYPE resume;