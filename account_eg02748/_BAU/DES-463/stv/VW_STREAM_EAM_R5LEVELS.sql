CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5LEVELS AS
                    SELECT
                        src:"LVL_LEVEL"::numeric(38, 10) AS LVL_LEVEL,
                        src:"LVL_UPDATECOUNT"::numeric(38, 0) AS LVL_UPDATECOUNT,
                        src:"LVL_LASTSAVED"::datetime AS LVL_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5LEVELS;