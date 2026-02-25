CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USEGRIDSYSDEFAULT AS
                    SELECT
                        src:"USD_GRIDID"::numeric(14, 0) AS USD_GRIDID,
                        src:"USD_USERID"::varchar(255) AS USD_USERID,
                        src:"USD_UPDATECOUNT"::numeric(38, 0) AS USD_UPDATECOUNT,
                        src:"USD_DATASPYID"::numeric(14, 0) AS USD_DATASPYID,
                        src:"USD_DATASPYFILTER"::varchar(50) AS USD_DATASPYFILTER,
                        src:"USD_LASTSAVED"::datetime AS USD_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USEGRIDSYSDEFAULT;