CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILURLMAPPING AS
                    SELECT
                        src:"MUM_TABLE"::varchar(30) AS MUM_TABLE,
                        src:"MUM_FUNCTION"::varchar(30) AS MUM_FUNCTION,
                        src:"MUM_JSPFIELD"::varchar(40) AS MUM_JSPFIELD,
                        src:"MUM_TABLECOLUMN"::varchar(155) AS MUM_TABLECOLUMN,
                        src:"MUM_LASTSAVED"::datetime AS MUM_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MAILURLMAPPING;