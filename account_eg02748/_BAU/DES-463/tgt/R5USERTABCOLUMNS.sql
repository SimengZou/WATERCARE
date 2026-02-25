CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_R5USERTABCOLUMNS(
                        UTC_TABLENAME varchar(30),
                        UTC_COLUMNNAME varchar(30),
                        UTC_DATATYPE varchar(106),
                        UTC_OBJ_XTYPE varchar(2),
                        UTC_COL_XTYPE numeric(38, 0),
                        UTC_COLLATION varchar(100),
                        UTC_ISID varchar(1),
                        UTC_DATABASE varchar(10),
                        UTC_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean
                    );