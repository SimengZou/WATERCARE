CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5PUBLICHOLIDAYS AS
                    SELECT
                        src:"AUTO"::numeric(24, 0) AS AUTO,
                        src:"HLY_ID"::varchar(30) AS HLY_ID,
                        src:"HLY_ORG"::varchar(15) AS HLY_ORG,
                        src:"HLY_CONCODE"::varchar(30) AS HLY_CONCODE,
                        src:"HLY_YEAR"::varchar(4) AS HLY_YEAR,
                        src:"HLY_DATE"::datetime AS HLY_DATE,
                        src:"HLY_NAME"::varchar(70) AS HLY_NAME,
                        src:"CREATEDBY"::varchar(255) AS CREATEDBY,
                        src:"CREATED"::datetime AS CREATED,
                        src:"UPDATEDBY"::varchar(255) AS UPDATEDBY,
                        src:"UPDATED"::datetime AS UPDATED,
                        src:"LASTSAVED"::datetime AS LASTSAVED,
                        src:"UPDATECOUNT"::numeric(38, 0) AS UPDATECOUNT
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_U5PUBLICHOLIDAYS;