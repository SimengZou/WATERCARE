CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_WSLCDRBUILD_XBUILDREVWCR
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_WSLCDRBUILD_XBUILDREVWCR')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_WSLCDRBUILD_XBUILDREVWCR() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_WSLCDRBUILD_XBUILDREVWCR resume;