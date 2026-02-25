CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_WSLCODES_PHONEAREACODE
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_WSLCODES_PHONEAREACODE')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_WSLCODES_PHONEAREACODE() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_WSLCODES_PHONEAREACODE resume;