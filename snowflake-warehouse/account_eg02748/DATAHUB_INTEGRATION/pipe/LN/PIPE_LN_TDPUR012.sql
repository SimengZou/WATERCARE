create or replace pipe DATAHUB_INTEGRATION.PIPE_LN_TDPUR012 auto_ingest=true
            error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_PIPE as
            copy into DATAHUB_LANDING.LN_TDPUR012
             from (
                SELECT
                $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                FROM @DATAHUB_INTEGRATION.STAGE_LN_TDPUR012(FILE_FORMAT => DATAHUB_INTEGRATION.json_format));