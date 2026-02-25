CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_R5ALERTHISTORY(
                        CSA_ORG varchar,
                        CSA_SCHEDULEITEM varchar,
                        CSA_PROJECT varchar,
                        CSA_ACTIVITY varchar,
                        CSA_CONTRACTCODE varchar,
                        CSA_SUPPLIER varchar,
                        CSA_CONTRACT varchar,
                        CSA_CONTRACTOR varchar,
                        CREATEDBY varchar,
                        UPDATEDBY varchar,
                        UPDATED datetime(0),
                        CREATED datetime(0),
						UPDATECOUNT numeric(38, 0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean
                    );