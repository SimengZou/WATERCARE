CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EVENTOBJECTS(
                        EOB_EVENT varchar(30),
                        EOB_OBTYPE varchar(4),
                        EOB_OBRTYPE varchar(4),
                        EOB_OBJECT varchar(30),
                        EOB_LEVEL smallint,
                        EOB_ROLLUP varchar(1),
                        EOB_DOWNTIME varchar(1),
                        EOB_OBJECT_ORG varchar(15),
                        EOB_UPDATECOUNT numeric(38, 0),
                        EOB_FROMPOINT numeric(24, 6),
                        EOB_TOPOINT numeric(24, 6),
                        EOB_WEIGHTPERCENTAGE numeric(5, 2),
                        EOB_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean,
      ETL_IS_DELETED boolean default false
                    );