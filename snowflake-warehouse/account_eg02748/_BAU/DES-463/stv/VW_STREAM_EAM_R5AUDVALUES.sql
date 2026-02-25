CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AUDVALUES AS
                    SELECT
                        src:"AVA_ATTRIBUTE"::numeric(38, 10) AS AVA_ATTRIBUTE,
                        src:"AVA_TABLE"::varchar(30) AS AVA_TABLE,
                        src:"AVA_PRIMARYID"::varchar(80) AS AVA_PRIMARYID,
                        src:"AVA_SECONDARYID"::varchar(180) AS AVA_SECONDARYID,
                        src:"AVA_FROM"::varchar(250) AS AVA_FROM,
                        src:"AVA_TO"::varchar(250) AS AVA_TO,
                        src:"AVA_CHANGED"::datetime AS AVA_CHANGED,
                        src:"AVA_MODIFIEDBY"::varchar(255) AS AVA_MODIFIEDBY,
                        src:"AVA_FUNCTION"::varchar(6) AS AVA_FUNCTION,
                        src:"AVA_UPDATED"::varchar(1) AS AVA_UPDATED,
                        src:"AVA_INSERTED"::varchar(1) AS AVA_INSERTED,
                        src:"AVA_DELETED"::varchar(1) AS AVA_DELETED,
                        src:"AVA_UPDATECOUNT"::numeric(38, 0) AS AVA_UPDATECOUNT,
                        src:"AVA_SCODE"::varchar(100) AS AVA_SCODE,
                        src:"AVA_MOBILE"::varchar(1) AS AVA_MOBILE,
                        src:"AVA_PRIMARYID2"::varchar(45) AS AVA_PRIMARYID2,
                        src:"AVA_LASTSAVED"::datetime AS AVA_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5AUDVALUES;