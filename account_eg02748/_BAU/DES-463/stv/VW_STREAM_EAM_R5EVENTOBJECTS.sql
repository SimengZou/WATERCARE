CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EVENTOBJECTS AS
                    SELECT
                        src:"EOB_EVENT"::varchar(30) AS EOB_EVENT,
                        src:"EOB_OBTYPE"::varchar(4) AS EOB_OBTYPE,
                        src:"EOB_OBRTYPE"::varchar(4) AS EOB_OBRTYPE,
                        src:"EOB_OBJECT"::varchar(30) AS EOB_OBJECT,
                        src:"EOB_LEVEL"::smallint AS EOB_LEVEL,
                        src:"EOB_ROLLUP"::varchar(1) AS EOB_ROLLUP,
                        src:"EOB_DOWNTIME"::varchar(1) AS EOB_DOWNTIME,
                        src:"EOB_OBJECT_ORG"::varchar(15) AS EOB_OBJECT_ORG,
                        src:"EOB_UPDATECOUNT"::numeric(38, 0) AS EOB_UPDATECOUNT,
                        src:"EOB_FROMPOINT"::numeric(24, 6) AS EOB_FROMPOINT,
                        src:"EOB_TOPOINT"::numeric(24, 6) AS EOB_TOPOINT,
                        src:"EOB_WEIGHTPERCENTAGE"::numeric(5, 2) AS EOB_WEIGHTPERCENTAGE,
                        src:"EOB_LASTSAVED"::datetime AS EOB_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EVENTOBJECTS;