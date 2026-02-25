CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTYVALUES AS
                    SELECT
                        src:"PRV_PROPERTY"::varchar(8) AS PRV_PROPERTY,
                        src:"PRV_RENTITY"::varchar(4) AS PRV_RENTITY,
                        src:"PRV_CLASS"::varchar(8) AS PRV_CLASS,
                        src:"PRV_CODE"::varchar(255) AS PRV_CODE,
                        src:"PRV_SEQNO"::numeric(38, 0) AS PRV_SEQNO,
                        src:"PRV_VALUE"::varchar(80) AS PRV_VALUE,
                        src:"PRV_NVALUE"::numeric(38, 10) AS PRV_NVALUE,
                        src:"PRV_DVALUE"::datetime AS PRV_DVALUE,
                        src:"PRV_CLASS_ORG"::varchar(15) AS PRV_CLASS_ORG,
                        src:"PRV_UPDATECOUNT"::numeric(38, 0) AS PRV_UPDATECOUNT,
                        src:"PRV_CREATED"::datetime AS PRV_CREATED,
                        src:"PRV_UPDATED"::datetime AS PRV_UPDATED,
                        src:"PRV_NOTUSED"::varchar(1) AS PRV_NOTUSED,
                        src:"PRV_LASTSAVED"::datetime AS PRV_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PROPERTYVALUES;