CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTHISTORY AS
                    SELECT
                        src:"ALH_ALERT"::varchar(30) AS ALH_ALERT,
                        src:"ALH_RSTATUS"::varchar(4) AS ALH_RSTATUS,
                        src:"ALH_RTYPE"::varchar(4) AS ALH_RTYPE,
                        src:"ALH_ENTITYCODE"::varchar(200) AS ALH_ENTITYCODE,
                        src:"ALH_ENTITYORG"::varchar(200) AS ALH_ENTITYORG,
                        src:"ALH_TEMPLATE"::varchar(18) AS ALH_TEMPLATE,
                        src:"ALH_RESULTCODE"::varchar(30) AS ALH_RESULTCODE,
                        src:"ALH_RESULTORG"::varchar(15) AS ALH_RESULTORG,
                        src:"ALH_ERRORMESSAGE"::varchar(2000) AS ALH_ERRORMESSAGE,
                        src:"ALH_CREATED"::datetime AS ALH_CREATED,
                        src:"ALH_UPDATECOUNT"::numeric(38, 0) AS ALH_UPDATECOUNT,
                        src:"ALH_LASTSAVED"::datetime AS ALH_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTHISTORY;