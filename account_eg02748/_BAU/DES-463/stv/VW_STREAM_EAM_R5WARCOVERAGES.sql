CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WARCOVERAGES AS
                    SELECT
                        src:"WCV_WARRANTY"::varchar(30) AS WCV_WARRANTY,
                        src:"WCV_OBJECT"::varchar(30) AS WCV_OBJECT,
                        src:"WCV_OBTYPE"::varchar(4) AS WCV_OBTYPE,
                        src:"WCV_OBRTYPE"::varchar(4) AS WCV_OBRTYPE,
                        src:"WCV_UOM"::varchar(30) AS WCV_UOM,
                        src:"WCV_DURATION"::numeric(9, 2) AS WCV_DURATION,
                        src:"WCV_EXPIRATION"::numeric(9, 2) AS WCV_EXPIRATION,
                        src:"WCV_EXPIRATIONDATE"::datetime AS WCV_EXPIRATIONDATE,
                        src:"WCV_NEARTHRESHOLD"::numeric(24, 6) AS WCV_NEARTHRESHOLD,
                        src:"WCV_STARTUSAGE"::numeric(24, 6) AS WCV_STARTUSAGE,
                        src:"WCV_SEQNO"::numeric(38, 10) AS WCV_SEQNO,
                        src:"WCV_ACTIVE"::varchar(1) AS WCV_ACTIVE,
                        src:"WCV_STARTDATE"::datetime AS WCV_STARTDATE,
                        src:"WCV_RECORDED"::datetime AS WCV_RECORDED,
                        src:"WCV_USER"::varchar(255) AS WCV_USER,
                        src:"WCV_OBJECT_ORG"::varchar(15) AS WCV_OBJECT_ORG,
                        src:"WCV_UPDATECOUNT"::numeric(38, 0) AS WCV_UPDATECOUNT,
                        src:"WCV_CREATED"::datetime AS WCV_CREATED,
                        src:"WCV_UPDATED"::datetime AS WCV_UPDATED,
                        src:"WCV_LASTSAVED"::datetime AS WCV_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WARCOVERAGES;