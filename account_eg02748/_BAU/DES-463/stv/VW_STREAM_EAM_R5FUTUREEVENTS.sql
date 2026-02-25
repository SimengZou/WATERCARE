CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FUTUREEVENTS AS
                    SELECT
                        src:"FUT_EVENT"::varchar(30) AS FUT_EVENT,
                        src:"FUT_SEQNO"::smallint AS FUT_SEQNO,
                        src:"FUT_DATE"::datetime AS FUT_DATE,
                        src:"FUT_UPDATECOUNT"::numeric(38, 0) AS FUT_UPDATECOUNT,
                        src:"FUT_MP_SEQ"::int AS FUT_MP_SEQ,
                        src:"FUT_LASTSAVED"::datetime AS FUT_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5FUTUREEVENTS;