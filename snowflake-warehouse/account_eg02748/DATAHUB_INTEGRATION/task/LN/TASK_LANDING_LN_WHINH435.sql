CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_LANDING_LN_WHINH435
            schedule  = 'USING CRON */30 * * * * UTC'
	        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_TASK
            as
            
            copy into DATAHUB_LANDING.LN_WHINH435
             from (
                SELECT
                $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                FROM @DATAHUB_INTEGRATION.STAGE_LN_WHINH435(FILE_FORMAT => DATAHUB_INTEGRATION.json_format));