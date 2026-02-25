create or replace pipe DATAHUB_INTEGRATION.PIPE_EAM_R5ORGYEARS auto_ingest=true
            error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_PIPE as
            copy into DATAHUB_LANDING.EAM_R5ORGYEARS
             from (
                SELECT
                $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                FROM @DATAHUB_INTEGRATION.STAGE_EAM_R5ORGYEARS(FILE_FORMAT => DATAHUB_INTEGRATION.json_format));