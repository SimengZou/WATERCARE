CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_R5MOBILETABLEUPDATES(
                        MTU_TABLENAME varchar(30),
                        MTU_UPDATED datetime(0),
                        MTU_RENTITY varchar(4),
                        MTU_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean
                    );