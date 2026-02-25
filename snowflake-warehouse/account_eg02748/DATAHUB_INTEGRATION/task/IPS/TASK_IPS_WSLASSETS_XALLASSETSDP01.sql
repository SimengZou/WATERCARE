CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_WSLASSETS_XALLASSETSDP01
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_WSLASSETS_XALLASSETSDP01')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_WSLASSETS_XALLASSETSDP01() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_WSLASSETS_XALLASSETSDP01 resume;