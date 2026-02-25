CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERTABCOLUMNS AS
                    SELECT
                        src:"UTC_TABLENAME"::varchar(30) AS UTC_TABLENAME,
                        src:"UTC_COLUMNNAME"::varchar(30) AS UTC_COLUMNNAME,
                        src:"UTC_DATATYPE"::varchar(106) AS UTC_DATATYPE,
                        src:"UTC_OBJ_XTYPE"::varchar(2) AS UTC_OBJ_XTYPE,
                        src:"UTC_COL_XTYPE"::numeric(38, 0) AS UTC_COL_XTYPE,
                        src:"UTC_COLLATION"::varchar(100) AS UTC_COLLATION,
                        src:"UTC_ISID"::varchar(1) AS UTC_ISID,
                        src:"UTC_DATABASE"::varchar(10) AS UTC_DATABASE,
                        src:"UTC_LASTSAVED"::datetime AS UTC_LASTSAVED
                        ,src:"_LASTSAVED"::datetime as ETL_LASTSAVED,
                        src:"_DELETED"::BOOLEAN as ETL_DELETED,
                        etl_load_datetime,
                       etl_load_metadatafilename
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERTABCOLUMNS;