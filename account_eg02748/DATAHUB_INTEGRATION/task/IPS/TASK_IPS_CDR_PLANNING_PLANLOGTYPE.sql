CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_CDR_PLANNING_PLANLOGTYPE
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_CDR_PLANNING_PLANLOGTYPE')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_CDR_PLANNING_PLANLOGTYPE() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_CDR_PLANNING_PLANLOGTYPE resume;