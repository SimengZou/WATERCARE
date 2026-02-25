CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_IPS_BILLING_ACCOUNTSERVICE
            schedule  = 'USING CRON 0 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            when
            system$stream_has_data('STREAM_IPS_BILLING_ACCOUNTSERVICE')
            as 
            call DATAHUB_INTEGRATION.SP_IPS_BILLING_ACCOUNTSERVICE() ;
            alter task DATAHUB_INTEGRATION.TASK_IPS_BILLING_ACCOUNTSERVICE resume;