create or replace task DATAHUB_EXTRACT.TASK_SM_MECHANICAL_METERS
	schedule='USING CRON 15 7-11 * * * Pacific/Auckland'
	error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
	when system$stream_has_data('DATAHUB_EXTRACT.STREAM_SM_MECHANICAL_METERS')
	as CALL DATAHUB_EXTRACT.SP_SM_MECHANICAL_METERS();
        alter task DATAHUB_EXTRACT.TASK_SM_MECHANICAL_METERS resume;