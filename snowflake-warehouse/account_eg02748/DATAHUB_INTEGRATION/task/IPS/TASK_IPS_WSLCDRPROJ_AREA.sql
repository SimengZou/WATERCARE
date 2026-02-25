CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_WSLCDRPROJ_AREA
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_WSLCDRPROJ_AREA')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_WSLCDRPROJ_AREA() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_WSLCDRPROJ_AREA resume;