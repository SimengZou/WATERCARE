create or replace pipe DATAHUB_INTEGRATION.PIPE_SM_CURATED_ALARMSEVENTS
        auto_ingest=true 
        error_integration = ${buildvar.env}_NOTIFICATION_INTEGRATION_PIPE
        aws_sns_topic='arn:aws:sns:${buildvar.smcuratedsns}:s3-event-notification-curatedtopic' as 
        copy into DATAHUB_LANDING.SM_CURATED_ALARMSEVENTS
            from
                (
                SELECT
                    $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                FROM 
                    @${buildvar.env}_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.STAGE_SM_CURATED/alarms_events/yyyy=(FILE_FORMAT => DATAHUB_INTEGRATION.parquet_format)
                )
        ON_ERROR=CONTINUE;