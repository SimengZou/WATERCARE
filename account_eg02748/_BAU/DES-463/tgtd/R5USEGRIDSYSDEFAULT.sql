CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USEGRIDSYSDEFAULT(
                        USD_GRIDID numeric(14, 0),
                        USD_USERID varchar(255),
                        USD_UPDATECOUNT numeric(38, 0),
                        USD_DATASPYID numeric(14, 0),
                        USD_DATASPYFILTER varchar(50),
                        USD_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean,
      ETL_IS_DELETED boolean default false
                    );