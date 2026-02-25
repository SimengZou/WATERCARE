CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PROPERTYVALUES(
                        PRV_PROPERTY varchar(8),
                        PRV_RENTITY varchar(4),
                        PRV_CLASS varchar(8),
                        PRV_CODE varchar(255),
                        PRV_SEQNO numeric(38, 0),
                        PRV_VALUE varchar(80),
                        PRV_NVALUE numeric(38, 10),
                        PRV_DVALUE datetime(0),
                        PRV_CLASS_ORG varchar(15),
                        PRV_UPDATECOUNT numeric(38, 0),
                        PRV_CREATED datetime(0),
                        PRV_UPDATED datetime(0),
                        PRV_NOTUSED varchar(1),
                        PRV_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean,
      ETL_IS_DELETED boolean default false
                    );