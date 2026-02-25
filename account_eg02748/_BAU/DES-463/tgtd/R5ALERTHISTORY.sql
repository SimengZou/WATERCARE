CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTHISTORY(
                        ALH_ALERT varchar(30),
                        ALH_RSTATUS varchar(4),
                        ALH_RTYPE varchar(4),
                        ALH_ENTITYCODE varchar(200),
                        ALH_ENTITYORG varchar(200),
                        ALH_TEMPLATE varchar(18),
                        ALH_RESULTCODE varchar(30),
                        ALH_RESULTORG varchar(15),
                        ALH_ERRORMESSAGE varchar(2000),
                        ALH_CREATED datetime(0),
                        ALH_UPDATECOUNT numeric(38, 0),
                        ALH_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean,
      ETL_IS_DELETED boolean default false
                    );