CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_U5DIMENSION3(
                        DIM3_CODE varchar(30),
                        DESCRIPTION varchar(80),
                        DIM3_NOTUSED varchar(1),
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