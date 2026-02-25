CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MAILURLMAPPING(
                        MUM_TABLE varchar(30),
                        MUM_FUNCTION varchar(30),
                        MUM_JSPFIELD varchar(40),
                        MUM_TABLECOLUMN varchar(155),
                        MUM_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean,
      ETL_IS_DELETED boolean default false
                    );