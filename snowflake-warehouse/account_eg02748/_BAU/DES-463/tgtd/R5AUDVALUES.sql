CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_R5AUDVALUES(
                        AVA_ATTRIBUTE numeric(38, 10),
                        AVA_TABLE varchar(30),
                        AVA_PRIMARYID varchar(80),
                        AVA_SECONDARYID varchar(180),
                        AVA_FROM varchar(250),
                        AVA_TO varchar(250),
                        AVA_CHANGED datetime(0),
                        AVA_MODIFIEDBY varchar(255),
                        AVA_FUNCTION varchar(6),
                        AVA_UPDATED varchar(1),
                        AVA_INSERTED varchar(1),
                        AVA_DELETED varchar(1),
                        AVA_UPDATECOUNT numeric(38, 0),
                        AVA_SCODE varchar(100),
                        AVA_MOBILE varchar(1),
                        AVA_PRIMARYID2 varchar(45),
                        AVA_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean,
      ETL_IS_DELETED boolean default false
                    );