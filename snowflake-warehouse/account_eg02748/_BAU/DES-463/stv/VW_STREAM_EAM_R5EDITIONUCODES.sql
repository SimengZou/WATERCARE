CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EDITIONUCODES AS
                    SELECT
                        src:"EDU_RCODE"::varchar(4) AS EDU_RCODE,
                        src:"EDU_CODE"::varchar(4) AS EDU_CODE,
                        src:"EDU_LANG"::varchar(2) AS EDU_LANG,
                        src:"EDU_DESC"::varchar(80) AS EDU_DESC,
                        src:"EDU_RENTITY"::varchar(4) AS EDU_RENTITY,
                        src:"EDU_MARKET"::varchar(8) AS EDU_MARKET,
                        src:"EDU_TEXTCODE"::varchar(12) AS EDU_TEXTCODE,
                        src:"EDU_LASTSAVED"::datetime AS EDU_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EDITIONUCODES;