CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILETABLEUPDATES AS
                    SELECT
                        src:"MTU_TABLENAME"::varchar(30) AS MTU_TABLENAME,
                        src:"MTU_UPDATED"::datetime AS MTU_UPDATED,
                        src:"MTU_RENTITY"::varchar(4) AS MTU_RENTITY,
                        src:"MTU_LASTSAVED"::datetime AS MTU_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILETABLEUPDATES;