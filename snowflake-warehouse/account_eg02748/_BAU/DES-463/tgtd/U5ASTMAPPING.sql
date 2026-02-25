CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_U5ASTMAPPING(
                        AUTOID numeric(24, 0),
                        AST_ID varchar(30),
                        AST_CLASS varchar(15),
                        AST_CLASSDESC varchar(100),
                        AST_ATTRIBUTE varchar(30),
                        AST_ATTRBDESC varchar(100),
                        CREATEDBY varchar(255),
                        CREATED datetime(0),
                        UPDATEDBY varchar(255),
                        UPDATED datetime(0),
                        LASTSAVED datetime(0),
                        UPDATECOUNT numeric(38, 0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean,
      ETL_IS_DELETED boolean default false
                    );