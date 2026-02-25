CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_U5PUBLICHOLIDAYS(
                        AUTO numeric(24, 0),
                        HLY_ID varchar(30),
                        HLY_ORG varchar(15),
                        HLY_CONCODE varchar(30),
                        HLY_YEAR varchar(4),
                        HLY_DATE datetime(0),
                        HLY_NAME varchar(70),
                        CREATEDBY varchar(255),
                        CREATED datetime(0),
                        UPDATEDBY varchar(255),
                        UPDATED datetime(0),
                        LASTSAVED datetime(0),
                        UPDATECOUNT numeric(38, 0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean
                    );