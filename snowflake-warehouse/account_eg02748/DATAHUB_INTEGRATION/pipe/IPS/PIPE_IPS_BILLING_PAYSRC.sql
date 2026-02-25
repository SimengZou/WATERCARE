create or replace pipe DATAHUB_INTEGRATION.PIPE_IPS_BILLING_PAYSRC auto_ingest=true
            error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_PIPE as
            copy into DATAHUB_LANDING.IPS_BILLING_PAYSRC
             from (
                SELECT
                $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                FROM @DATAHUB_INTEGRATION.STAGE_IPS_BILLING_PAYSRC(FILE_FORMAT => DATAHUB_INTEGRATION.json_format));