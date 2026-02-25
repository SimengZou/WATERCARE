CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_DEPM_AMP
            schedule  = 'USING CRON 0 6 * * * Pacific/Auckland'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_DEPM_AMP')
            as 
            call DATAHUB_INTEGRATION.SP_DEPM_LOAD_TARGET('DEPM_AMP','TIMESTAMP') ;
            alter task DATAHUB_INTEGRATION.TASK_DEPM_AMP resume;