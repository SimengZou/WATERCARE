CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5DIMENSION3 AS
                    SELECT
                        src:"DIM3_CODE"::varchar(30) AS DIM3_CODE,
                        src:"DESCRIPTION"::varchar(80) AS DESCRIPTION,
                        src:"DIM3_NOTUSED"::varchar(1) AS DIM3_NOTUSED,
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
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_U5DIMENSION3;