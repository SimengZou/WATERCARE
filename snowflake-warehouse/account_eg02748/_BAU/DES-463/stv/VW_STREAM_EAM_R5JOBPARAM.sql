CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5JOBPARAM AS
                    SELECT
                        src:"JPR_NAME"::varchar(10) AS JPR_NAME,
                        src:"JPR_LINE"::int AS JPR_LINE,
                        src:"JPR_PARAMETER"::varchar(20) AS JPR_PARAMETER,
                        src:"JPR_RENTITY"::varchar(4) AS JPR_RENTITY,
                        src:"JPR_RTYPE"::varchar(4) AS JPR_RTYPE,
                        src:"JPR_DATATYPE"::varchar(2) AS JPR_DATATYPE,
                        src:"JPR_LENGTH"::numeric(10, 0) AS JPR_LENGTH,
                        src:"JPR_FORMAT"::varchar(20) AS JPR_FORMAT,
                        src:"JPR_DEFAULT"::varchar(30) AS JPR_DEFAULT,
                        src:"JPR_FIXED"::varchar(1) AS JPR_FIXED,
                        src:"JPR_MANDATORY"::varchar(1) AS JPR_MANDATORY,
                        src:"JPR_UPPER"::varchar(1) AS JPR_UPPER,
                        src:"JPR_OPTIONS"::numeric(10, 0) AS JPR_OPTIONS,
                        src:"JPR_REMEMBER"::varchar(1) AS JPR_REMEMBER,
                        src:"JPR_TEST"::varchar(24) AS JPR_TEST,
                        src:"JPR_QUERY"::varchar(8) AS JPR_QUERY,
                        src:"JPR_LOVFUNCTION"::varchar(6) AS JPR_LOVFUNCTION,
                        src:"JPR_PROPERTY"::varchar(8) AS JPR_PROPERTY,
                        src:"JPR_EWSLOVDEF"::varchar(200) AS JPR_EWSLOVDEF,
                        src:"JPR_UPDATECOUNT"::numeric(38, 0) AS JPR_UPDATECOUNT,
                        src:"JPR_LASTSAVED"::datetime AS JPR_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5JOBPARAM;