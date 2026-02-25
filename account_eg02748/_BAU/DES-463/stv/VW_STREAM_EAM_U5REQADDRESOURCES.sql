CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5REQADDRESOURCES AS
                    SELECT
                        src:"RAR_TRANSID"::varchar(50) AS RAR_TRANSID,
                        src:"RAR_EVENT"::varchar(30) AS RAR_EVENT,
                        src:"RAR_TYPE"::varchar(30) AS RAR_TYPE,
                        src:"RAR_NOTES"::varchar(512) AS RAR_NOTES,
                        src:"RAR_HAZARDS"::varchar(30) AS RAR_HAZARDS,
                        src:"RAR_RESULTWONUM"::varchar(30) AS RAR_RESULTWONUM,
                        src:"RAR_RAISEDBY"::varchar(30) AS RAR_RAISEDBY,
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
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_U5REQADDRESOURCES;