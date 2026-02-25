CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_R5VIOLATIONS(
                        VIO_USER varchar(255),
                        VIO_PASSWORD varchar(50),
                        VIO_DATE datetime(0),
                        VIO_OSUSER varchar(255),
                        VIO_OSMACHINE varchar(64),
                        VIO_UPDATECOUNT numeric(38, 0),
                        VIO_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean
                    );