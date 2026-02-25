CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_R5LEVELS(
                        LVL_LEVEL numeric(38, 10),
                        LVL_UPDATECOUNT numeric(38, 0),
                        LVL_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean
                    );