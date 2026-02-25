CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_WSLCRM_XRETAILAUDITDP
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_WSLCRM_XRETAILAUDITDP')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_WSLCRM_XRETAILAUDITDP() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_WSLCRM_XRETAILAUDITDP resume;