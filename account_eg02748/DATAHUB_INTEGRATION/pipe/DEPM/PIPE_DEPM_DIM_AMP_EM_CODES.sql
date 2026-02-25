create or replace pipe DATAHUB_INTEGRATION.PIPE_DEPM_DIM_AMP_EM_CODES auto_ingest=true
            error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_PIPE as
            copy into DATAHUB_LANDING.DEPM_DIM_AMP_EM_CODES
             from (
                SELECT
                $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                FROM @DATAHUB_INTEGRATION.STAGE_DEPM_DIM_AMP_EM_CODES(FILE_FORMAT => DATAHUB_INTEGRATION.json_format));