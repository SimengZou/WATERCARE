CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_DEPM_DIM_CAPEX_VERSION
            schedule  = 'USING CRON 0 6 * * * Pacific/Auckland'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_DEPM_DIM_CAPEX_VERSION')
            as 
            call DATAHUB_INTEGRATION.SP_DEPM_LOAD_TARGET('DEPM_DIM_CAPEX_VERSION','TIMESTAMP') ;
            alter task DATAHUB_INTEGRATION.TASK_DEPM_DIM_CAPEX_VERSION resume;