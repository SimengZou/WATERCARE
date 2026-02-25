CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_U5CICLAIMS(
                        CLI_TRANSID varchar(50),
                        CLI_EVENT varchar(30),
                        CLI_CONTRACTOR varchar(30),
                        CLI_LASTITEM varchar(1),
                        CLI_COST varchar,
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