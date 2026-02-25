CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_U5REQADDRESOURCES(
                        RAR_TRANSID varchar(50),
                        RAR_EVENT varchar(30),
                        RAR_TYPE varchar(30),
                        RAR_NOTES varchar(512),
                        RAR_HAZARDS varchar(30),
                        RAR_RESULTWONUM varchar(30),
                        RAR_RAISEDBY varchar(30),
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