CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5VIOLATIONS AS
                    SELECT
                        src:"VIO_USER"::varchar(255) AS VIO_USER,
                        src:"VIO_PASSWORD"::varchar(50) AS VIO_PASSWORD,
                        src:"VIO_DATE"::datetime AS VIO_DATE,
                        src:"VIO_OSUSER"::varchar(255) AS VIO_OSUSER,
                        src:"VIO_OSMACHINE"::varchar(64) AS VIO_OSMACHINE,
                        src:"VIO_UPDATECOUNT"::numeric(38, 0) AS VIO_UPDATECOUNT,
                        src:"VIO_LASTSAVED"::datetime AS VIO_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5VIOLATIONS;