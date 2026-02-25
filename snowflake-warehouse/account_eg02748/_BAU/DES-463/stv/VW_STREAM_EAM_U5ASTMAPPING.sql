CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5ASTMAPPING AS
                    SELECT
                        src:"AUTOID"::numeric(24, 0) AS AUTOID,
                        src:"AST_ID"::varchar(30) AS AST_ID,
                        src:"AST_CLASS"::varchar(15) AS AST_CLASS,
                        src:"AST_CLASSDESC"::varchar(100) AS AST_CLASSDESC,
                        src:"AST_ATTRIBUTE"::varchar(30) AS AST_ATTRIBUTE,
                        src:"AST_ATTRBDESC"::varchar(100) AS AST_ATTRBDESC,
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
                    FROM DATAHUB_INTEGRATION.STREAM_EAM_U5ASTMAPPING;