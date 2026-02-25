CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CICLAIMS AS
                    SELECT
                        src:"CLI_TRANSID"::varchar(50) AS CLI_TRANSID,
                        src:"CLI_EVENT"::varchar(30) AS CLI_EVENT,
                        src:"CLI_CONTRACTOR"::varchar(30) AS CLI_CONTRACTOR,
                        src:"CLI_LASTITEM"::varchar(1) AS CLI_LASTITEM,
                        src:"CLI_COST"::varchar AS CLI_COST,
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
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_U5CICLAIMS;