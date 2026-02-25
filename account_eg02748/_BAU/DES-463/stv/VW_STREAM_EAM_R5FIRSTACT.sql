CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FIRSTACT AS
                    SELECT
                        src:"ACT_EVENT"::varchar(30) AS ACT_EVENT,
                        src:"ACT_ACT"::int AS ACT_ACT,
                        src:"ACT_START"::datetime AS ACT_START,
                        src:"ACT_TRADE"::varchar(15) AS ACT_TRADE,
                        src:"ACT_PERSONS"::int AS ACT_PERSONS,
                        src:"ACT_DURATION"::int AS ACT_DURATION,
                        src:"ACT_EST"::numeric(38, 10) AS ACT_EST,
                        src:"ACT_REM"::numeric(38, 10) AS ACT_REM,
                        src:"ACT_TASK"::varchar(20) AS ACT_TASK,
                        src:"ACT_MATLIST"::varchar(20) AS ACT_MATLIST,
                        src:"ACT_MULTIPLETRADES"::varchar(1) AS ACT_MULTIPLETRADES,
                        src:"ACT_RPC"::varchar(4) AS ACT_RPC,
                        src:"ACT_WAP"::varchar(4) AS ACT_WAP,
                        src:"ACT_TPF"::varchar(4) AS ACT_TPF,
                        src:"ACT_MANUFACT"::varchar(24) AS ACT_MANUFACT,
                        src:"ACT_SYSLEVEL"::varchar(30) AS ACT_SYSLEVEL,
                        src:"ACT_ASMLEVEL"::varchar(30) AS ACT_ASMLEVEL,
                        src:"ACT_COMPLEVEL"::varchar(30) AS ACT_COMPLEVEL,
                        src:"ACT_COMPLOCATION"::varchar(80) AS ACT_COMPLOCATION,
                        src:"ACT_LASTSAVED"::datetime AS ACT_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5FIRSTACT;