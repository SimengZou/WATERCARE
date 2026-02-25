CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_R5FUTUREEVENTS(
                        FUT_EVENT varchar(30),
                        FUT_SEQNO smallint,
                        FUT_DATE datetime(0),
                        FUT_UPDATECOUNT numeric(38, 0),
                        FUT_MP_SEQ int,
                        FUT_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean
                    );