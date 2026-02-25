
                         copy into DATAHUB_LANDING.EAM_R5MRCS
                    from (
            SELECT
            $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
            FROM @DATAHUB_INTEGRATION.STAGE_EAM_R5MRCS(FILE_FORMAT => json_format));